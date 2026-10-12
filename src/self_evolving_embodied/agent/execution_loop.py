"""One Agent: plan -> governed tool invocation -> optional assessment -> decide.

The durable WorkflowOrchestrator owns dispatch, authorization, fencing and
recovery. This coordinator calls it only one step at a time so an evaluation
can influence the NEXT tool call. Neither LLM plans nor assessor conclusions
become grants, safety decisions, or environment ground truth.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ..core.contracts import DomainError
from ..core.ids import uid
from ..governance.runtime_policy import RuntimePolicyActivator, RuntimePolicyBinding
from ..planning.validator import PlanValidator
from ..runtime.workflow_orchestrator import WorkflowOrchestrator
from ..runtime.workflow_store import WorkflowSnapshot
from ..toolboxes.assessment import (
    AssessmentRequest, AssessmentResult, AssessmentToolbox, Outcome,
)
from ..tools.contracts import Effect
from ..observations import ObservationReader
from ..toolboxes.execution import ExecutionToolbox
from .adaptive_policy import AdaptivePolicyRouter, TrustedToolAlternative
from .contracts import (
    AgentAction, AgentDecision, AgentLoopReport, AssessmentRoute,
    BrainContext, SingleAgentBrain,
)


class SingleAgentExecutionLoop:
    """Stepwise coordinator around the existing durable WorkflowOrchestrator.

    - Only application code supplies tool->assessor routes.
    - No assessments or raw sensor records are written to the observation DB;
      short, structured assessment summaries go into the workflow event chain.
    - Physical actions only run through the existing authorized execution bridge.
    - Semantic UNKNOWN never advances the task automatically.
    - Replans create a new independently validated workflow; parent/child links
      are journaled. A crash during device dispatch follows existing quarantine.
    """

    def __init__(self, brain: SingleAgentBrain, workflow: WorkflowOrchestrator,
                 assessments: AssessmentToolbox,
                 routes: Sequence[AssessmentRoute] = (),
                 runtime_policy: RuntimePolicyBinding | None = None,
                 tool_alternatives: Sequence[TrustedToolAlternative] = (),
                 execution_toolbox: ExecutionToolbox | None = None,
                 observations: ObservationReader | None = None) -> None:
        if not workflow.require_agent_feedback:
            raise DomainError("AGENT_FEEDBACK_GATE_REQUIRED")
        self.brain = brain
        self.workflow = workflow
        self.assessments = assessments
        self.runtime_policy = runtime_policy
        if runtime_policy is not None:
            RuntimePolicyActivator(workflow.store).validate_binding(runtime_policy)
        if not all(isinstance(x, AssessmentRoute) for x in routes):
            raise DomainError("INVALID_ASSESSMENT_ROUTE")
        self._routes = {route.tool_id: route.assessor_id for route in routes}
        if len(self._routes) != len(routes):
            raise DomainError("DUPLICATE_ASSESSMENT_ROUTE")
        for tool_id, assessor_id in self._routes.items():
            workflow.registry.resolve(tool_id)
            assessments.resolve(assessor_id)
        self._router = AdaptivePolicyRouter(
            workflow.registry, assessments, routes, tool_alternatives,
            execution=execution_toolbox, observations=observations,
        )

    def _preflight(self, plan: Any) -> None:
        """Physical actions remain under Harness; controlled research stays read-only."""
        for step in plan.subtasks:
            spec = self.workflow.registry.resolve(step.tool_id)
            if self.runtime_policy is not None and spec.effect != Effect.READ_ONLY:
                raise DomainError("RESEARCH_RUNTIME_PHYSICAL_ACTION_FORBIDDEN")
            if spec.effect == Effect.PHYSICAL_ACTION and step.tool_id not in self._routes:
                raise DomainError("EFFECTFUL_ASSESSOR_ROUTE_REQUIRED")

    def _policy_pin(self) -> tuple[str, str] | None:
        binding = self.runtime_policy
        if binding is None:
            return None
        RuntimePolicyActivator(self.workflow.store).validate_binding(binding)
        return binding.activation_id, binding.manifest_hash

    def _check_pinned(self, run_id: str) -> None:
        if self.workflow.workflows.policy_pin(run_id) != self._policy_pin():
            raise DomainError("RUNTIME_POLICY_PIN_REQUIRED_OR_MISMATCHED")
        expected = self._router.manifest_hash
        if self.workflow.workflows.routing_hash(run_id) != expected:
            raise DomainError("RUNTIME_ROUTING_CONFIGURATION_CHANGED")

    def _max_replans(self, value: int) -> int:
        return (self.runtime_policy.planning_replan_limit(value)
                if self.runtime_policy is not None else value)

    def _check_tool_budget(self, root: str) -> bool:
        binding = self.runtime_policy
        if binding is None or binding.max_tool_calls is None:
            return True
        return self.workflow.workflows.dispatched_count(root) < binding.max_tool_calls

    def _uncertain_retry_allowed(self, run_id: str) -> bool:
        if self.runtime_policy is None:
            return True
        limits = (self.runtime_policy.uncertain_assessment_retry_limit,
                  self.runtime_policy.observation_retry_budget)
        applicable = [value for value in limits if value is not None]
        if not applicable:
            return True
        allowed = min(applicable)
        previous = self.workflow.workflows.agent_turns(run_id)
        uncertainty_count = sum(turn.get("outcome") == "UNKNOWN" for turn in previous)
        return uncertainty_count < allowed

    def _check_plan_budget(self, root: str | None, plan: Any) -> None:
        binding = self.runtime_policy
        if binding is None or binding.max_tool_calls is None:
            return
        already = self.workflow.workflows.dispatched_count(root) if root is not None else 0
        if already + len(plan.subtasks) > binding.max_tool_calls:
            raise DomainError("RUNTIME_POLICY_TOOL_BUDGET_EXCEEDED")

    def start(self, goal: str, *, run_id: str | None = None,
              max_turns: int = 40, max_replans: int = 2) -> AgentLoopReport:
        self._budgets(max_turns, max_replans)
        max_replans = self._max_replans(max_replans)
        pin = self._policy_pin()
        plan = self.brain.plan(goal)
        if plan.goal != goal or not PlanValidator(self.workflow.registry).is_validated(plan):
            raise DomainError("UNVALIDATED_AGENT_PLAN")
        plan = self._router.rewrite_plan(plan, self.runtime_policy)
        self._preflight(plan)
        self._router.pre_observation(plan, self.runtime_policy)
        self._check_plan_budget(None, plan)
        root = run_id or uid()
        # A single dispatch maximum; no further tool action before loop review.
        snapshot = self.workflow.run(plan, run_id=root, max_invocations=1,
                                     policy_pin=pin,
                                     routing_manifest=self._router.manifest())
        return self._drive(root, snapshot.run_id, 0, max_turns, max_replans,
                           retry_observation=False)

    def resume(self, root_run_id: str, *, max_turns: int = 40,
               max_replans: int = 2, retry_observation: bool = False) -> AgentLoopReport:
        """Recover journaled decisions before dispatching any more tool calls.

        retry_observation=True allows one NEW assessment attempt for a paused
        step; consumers should invoke it only after fresh sensor data arrives.
        """
        self._budgets(max_turns, max_replans)
        max_replans = self._max_replans(max_replans)
        self._check_pinned(root_run_id)
        active = root_run_id
        revisions = 0
        visited: set[str] = set()
        while True:
            if active in visited:
                raise DomainError("AGENT_REPLAN_CYCLE")
            visited.add(active)
            successor = self.workflow.workflows.replan_successor(active)
            if successor is None:
                break
            self._check_pinned(successor)
            active = successor
            revisions += 1
            if revisions > max_replans:
                raise DomainError("AGENT_REPLAN_BUDGET_EXCEEDED")
        return self._drive(root_run_id, active, revisions, max_turns, max_replans,
                           retry_observation=retry_observation)

    @staticmethod
    def _budgets(max_turns: int, max_replans: int) -> None:
        if type(max_turns) is not int or not 1 <= max_turns <= 100:
            raise DomainError("INVALID_AGENT_TURN_BUDGET")
        if type(max_replans) is not int or not 0 <= max_replans <= 10:
            raise DomainError("INVALID_AGENT_REPLAN_BUDGET")

    def _report(self, root: str, active: str, status: str, revisions: int,
                last_outcome: str = "NOT_ASSESSED", reason: str | None = None) -> AgentLoopReport:
        snapshot = self.workflow.workflows.snapshot(active)
        turns = self.workflow.workflows.agent_turns(active)
        if turns and last_outcome == "NOT_ASSESSED":
            last_outcome = turns[-1].get("outcome", "NOT_ASSESSED")
        return AgentLoopReport(root, active, status, snapshot.status,
                               len(turns), revisions, last_outcome, reason)

    def _assess(self, step: dict[str, Any], goal: str,
                record: dict[str, Any]) -> AssessmentResult | None:
        assessor_id = self._router.select_assessor(step["tool_id"], self.runtime_policy)
        if assessor_id is None:
            return None
        request = AssessmentRequest(
            subtask_id=step["id"], goal=goal[:1000],
            execution_status="REPORTED_COMPLETED",
            tool_report=record["result"] or {},
        )
        try:
            return self.assessments.assess(
                assessor_id, request,
                max_age_ms=(self.runtime_policy.max_observation_age_ms
                            if self.runtime_policy is not None else None))
        except DomainError as exc:
            # Provider availability is not evidence of task failure or success.
            return AssessmentResult(Outcome.UNKNOWN, assessor_id,
                                    f"Assessment unavailable: {exc.code}")

    def _context(self, goal: str, step: dict[str, Any], record: dict[str, Any],
                 assessment: dict[str, Any] | None, revisions: int) -> BrainContext:
        tool_report = record["result"] or (
            {"failure_code": record.get("failure_code")}
            if record["status"] == "BLOCKED" else {}
        )
        return BrainContext(
            goal=goal, subtask_id=step["id"], tool_id=step["tool_id"],
            execution_status=record["status"], tool_report=tool_report,
            assessment=assessment, plan_revision=revisions,
        )

    def _guard(self, step: dict[str, Any], assessment: AssessmentResult | None,
               proposed: AgentDecision) -> AgentDecision:
        """Trusted policy overrides unsafe LLM requests; LLM cannot waive it."""
        effect = self.workflow.registry.resolve(step["tool_id"]).effect
        if proposed.action == AgentAction.STOP:
            return proposed
        if assessment is None and effect == Effect.PHYSICAL_ACTION:
            return AgentDecision(AgentAction.WAIT_FOR_OBSERVATION,
                                 "Physical subtask has no configured outcome assessor")
        if assessment is not None and assessment.outcome == Outcome.UNKNOWN:
            return AgentDecision(AgentAction.WAIT_FOR_OBSERVATION,
                                 "Task outcome remains unknown under available observations")
        if (assessment is not None and assessment.outcome == Outcome.NOT_SUPPORTED):
            if effect == Effect.PHYSICAL_ACTION:
                return AgentDecision(AgentAction.WAIT_FOR_OBSERVATION,
                                     "Physical goal not supported; inspect state before more actions")
            if proposed.action == AgentAction.CONTINUE:
                return AgentDecision(AgentAction.REPLAN,
                                     "Assessment does not support subtask goal")
        return proposed

    def _record(self, run_id: str, step: dict[str, Any],
                assessment: AssessmentResult | None, decision: AgentDecision,
                execution_status: str = "SUCCEEDED") -> None:
        result = assessment.to_dict() if assessment is not None else None
        if result is not None and len(result["observation_refs"]) > 8:
            result = {**result, "observation_refs": result["observation_refs"][:8],
                      "reason": result["reason"][:600] + " [references truncated]"}
        payload = {
            "subtask_id": step["id"], "tool_id": step["tool_id"],
            "execution_status": execution_status,
            "outcome": assessment.outcome.value if assessment else "NOT_ASSESSED",
            "assessment": result, "action": decision.action.value,
            "reason": decision.reason,
        }
        with self.workflow.store.worker_session():
            self.workflow.workflows.record_agent_turn(run_id, payload)

    def _replan(self, root: str, active: str, revisions: int,
                context: BrainContext, max_replans: int) -> str | None:
        if revisions >= max_replans:
            return None
        plan = self.brain.replan(context.goal, context)
        if plan.goal != context.goal or not PlanValidator(self.workflow.registry).is_validated(plan):
            raise DomainError("UNVALIDATED_AGENT_REPLAN")
        plan = self._router.rewrite_plan(plan, self.runtime_policy)
        self._preflight(plan)
        self._router.pre_observation(plan, self.runtime_policy)
        self._check_pinned(active)
        self._check_plan_budget(root, plan)
        # Candidate workflow exists durably before parent is marked superseded;
        # nothing is dispatched in this transaction block.
        next_run = uid()
        with self.workflow.store.worker_session():
            self.workflow.workflows.create_replan(
                active, next_run, plan, self.workflow.catalog_hash(),
                policy_pin=self._policy_pin(),
                routing_manifest=self._router.manifest(),
            )
        return next_run

    def _drive(self, root: str, active: str, revisions: int,
               max_turns: int, max_replans: int,
               *, retry_observation: bool) -> AgentLoopReport:
        # Bound the number of dispatch/decision transitions per API call.
        for _ in range(max_turns):
            self._check_pinned(active)
            if retry_observation and not self._uncertain_retry_allowed(active):
                return self._report(root, active, "WAITING_OBSERVATION", revisions,
                                    reason="RUNTIME_POLICY_ASSESSMENT_RETRY_LIMIT")
            snapshot = self.workflow.workflows.snapshot(active)
            if snapshot.status == "BLOCKED" and snapshot.failure_code in {
                "READ_ONLY_TOOL_FAILED", "SUBTASK_VERIFICATION_REJECTED"
            }:
                # Known failed READ_ONLY calls can be replanned; timed-out or
                # ambiguous calls, and ALL effectful uncertainties, cannot.
                stored_plan, _ = self.workflow.workflows.load_plan(active)
                failed = next((step for step in stored_plan["subtasks"]
                               if snapshot.subtask_states[step["id"]] == "BLOCKED"), None)
                if failed is None or self.workflow.registry.resolve(failed["tool_id"]).effect != Effect.READ_ONLY:
                    return self._report(root, active, "HALTED", revisions,
                                        reason=snapshot.failure_code)
                prior_turns = self.workflow.workflows.agent_turns(active)
                previous_turn = next((turn for turn in reversed(prior_turns)
                                      if turn["subtask_id"] == failed["id"]), None)
                record = self.workflow.workflows.subtask_record(active, failed["id"])
                context = self._context(stored_plan["goal"], failed, record, None, revisions)
                if previous_turn is None:
                    try:
                        proposed = self.brain.decide(context)
                        if not isinstance(proposed, AgentDecision):
                            raise DomainError("INVALID_AGENT_DECISION")
                    except DomainError as exc:
                        return self._report(root, active, "PAUSED", revisions,
                                            reason=f"BRAIN_DECISION_UNAVAILABLE:{exc.code}")
                    if proposed.action == AgentAction.CONTINUE:
                        proposed = AgentDecision(AgentAction.REPLAN,
                                                 "Known failed read-only step requires new plan")
                    try:
                        self._record(active, failed, None, proposed, execution_status="BLOCKED")
                    except DomainError as exc:
                        return self._report(root, active, "PAUSED", revisions,
                                            reason=f"AGENT_FEEDBACK_NOT_RECORDED:{exc.code}")
                    previous_turn = {"action": proposed.action.value, "outcome": "NOT_ASSESSED",
                                     "reason": proposed.reason}
                if previous_turn["action"] == "REPLAN":
                    try:
                        successor = self._replan(root, active, revisions, context, max_replans)
                    except DomainError as exc:
                        return self._report(root, active, "PAUSED", revisions,
                                            reason=f"REPLAN_UNAVAILABLE:{exc.code}")
                    if successor is None:
                        return self._report(root, active, "REPLAN_BUDGET_REACHED", revisions)
                    active, revisions = successor, revisions + 1
                    if not self._check_tool_budget(root):
                        return self._report(root, active, "POLICY_TOOL_BUDGET_REACHED", revisions)
                    try:
                        self.workflow.resume(active, max_invocations=1)
                    except DomainError as exc:
                        return self._report(root, active, "PAUSED", revisions,
                                            reason=f"WORKFLOW_UNAVAILABLE:{exc.code}")
                    continue
                status = ("WAITING_OBSERVATION" if previous_turn["action"] ==
                          "WAIT_FOR_OBSERVATION" else "HALTED")
                return self._report(root, active, status, revisions,
                                    reason=previous_turn["reason"])
            if snapshot.status in {"BLOCKED", "HUMAN_REVIEW"}:
                return self._report(root, active, "HALTED", revisions,
                                    reason=snapshot.failure_code)
            plan, _catalog_hash = self.workflow.workflows.load_plan(active)
            goal = plan["goal"]
            prior = self.workflow.workflows.agent_turns(active)
            decisions = {turn["subtask_id"]: turn for turn in prior}
            followup: tuple[dict[str, Any], dict[str, Any], Mapping[str, Any]] | None = None
            for step in plan["subtasks"]:
                if snapshot.subtask_states[step["id"]] != "SUCCEEDED":
                    continue
                last = decisions.get(step["id"])
                if last is None or (last["action"] == "WAIT_FOR_OBSERVATION" and retry_observation):
                    record = self.workflow.workflows.subtask_record(active, step["id"])
                    assessment = self._assess(step, goal, record)
                    as_dict = assessment.to_dict() if assessment is not None else None
                    ctx = self._context(goal, step, record, as_dict, revisions)
                    try:
                        proposed = self.brain.decide(ctx)
                        if not isinstance(proposed, AgentDecision):
                            raise DomainError("INVALID_AGENT_DECISION")
                    except DomainError as exc:
                        return self._report(root, active, "PAUSED", revisions,
                                            reason=f"BRAIN_DECISION_UNAVAILABLE:{exc.code}")
                    decision = self._guard(step, assessment, proposed)
                    try:
                        self._record(active, step, assessment, decision)
                    except DomainError as exc:
                        return self._report(root, active, "PAUSED", revisions,
                                            reason=f"AGENT_FEEDBACK_NOT_RECORDED:{exc.code}")
                    last = {"subtask_id": step["id"], "action": decision.action.value,
                            "assessment": as_dict, "outcome": (
                                assessment.outcome.value if assessment else "NOT_ASSESSED"),
                            "reason": decision.reason}
                    retry_observation = False
                if last["action"] == "WAIT_FOR_OBSERVATION":
                    return self._report(root, active, "WAITING_OBSERVATION", revisions,
                                        last["outcome"], last.get("reason"))
                if last["action"] == "STOP":
                    with self.workflow.store.worker_session():
                        if self.workflow.workflows.snapshot(active).status not in {
                                "BLOCKED", "HUMAN_REVIEW", "SUCCEEDED"}:
                            self.workflow.workflows.block(active, "BRAIN_REQUESTED_STOP")
                    return self._report(root, active, "HALTED", revisions,
                                        last["outcome"], "BRAIN_REQUESTED_STOP")
                if last["action"] == "REPLAN":
                    record = self.workflow.workflows.subtask_record(active, step["id"])
                    context = self._context(goal, step, record, last["assessment"], revisions)
                    followup = (step, record, last)
                    try:
                        next_run = self._replan(root, active, revisions, context, max_replans)
                    except DomainError as exc:
                        return self._report(root, active, "PAUSED", revisions,
                                            last["outcome"], f"REPLAN_UNAVAILABLE:{exc.code}")
                    if next_run is None:
                        return self._report(root, active, "REPLAN_BUDGET_REACHED", revisions,
                                            last["outcome"])
                    active = next_run
                    revisions += 1
                    break
            if followup is not None:
                if not self._check_tool_budget(root):
                    return self._report(root, active, "POLICY_TOOL_BUDGET_REACHED", revisions)
                try:
                    dispatched = self.workflow.resume(active, max_invocations=1)
                except DomainError as exc:
                    return self._report(root, active, "PAUSED", revisions,
                                        reason=f"WORKFLOW_UNAVAILABLE:{exc.code}")
                if dispatched.status == "HUMAN_REVIEW" or (
                    dispatched.status == "BLOCKED" and dispatched.failure_code not in {
                        "READ_ONLY_TOOL_FAILED", "SUBTASK_VERIFICATION_REJECTED"
                    }
                ):
                    return self._report(root, active, "HALTED", revisions,
                                        reason=dispatched.failure_code)
                continue
            if snapshot.status == "SUCCEEDED":
                # This means declared workflow steps passed Harness verification;
                # it does NOT assert that the user's physical goal is true.
                return self._report(root, active, "EXECUTION_WORKFLOW_COMPLETED", revisions)
            # `prior` was captured before this iteration assessed/resumed a step.
            # Use the latest journaled decision so that fresh evidence can clear
            # a previous WAIT_FOR_OBSERVATION instead of replaying a stale pause.
            current_turns = self.workflow.workflows.agent_turns(active)
            if current_turns and current_turns[-1]["action"] == "WAIT_FOR_OBSERVATION":
                return self._report(root, active, "WAITING_OBSERVATION", revisions,
                                    current_turns[-1]["outcome"], current_turns[-1]["reason"])
            previous = (snapshot.status, snapshot.event_count)
            if not self._check_tool_budget(root):
                return self._report(root, active, "POLICY_TOOL_BUDGET_REACHED", revisions)
            try:
                dispatched = self.workflow.resume(active, max_invocations=1)
            except DomainError as exc:
                return self._report(root, active, "PAUSED", revisions,
                                    reason=f"WORKFLOW_UNAVAILABLE:{exc.code}")
            if dispatched.status == "HUMAN_REVIEW" or (
                dispatched.status == "BLOCKED" and dispatched.failure_code not in {
                    "READ_ONLY_TOOL_FAILED", "SUBTASK_VERIFICATION_REJECTED"
                }
            ):
                return self._report(root, active, "HALTED", revisions,
                                    reason=dispatched.failure_code)
            if previous == (dispatched.status, dispatched.event_count) or dispatched.status.startswith("WAITING"):
                return self._report(root, active, "PAUSED", revisions,
                                    reason=dispatched.failure_code)
        return self._report(root, active, "TURN_BUDGET_REACHED", revisions)
