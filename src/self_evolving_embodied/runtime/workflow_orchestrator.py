"""Durable, dependency-aware orchestration for registered model tools.

Read-only model tools and explicitly granted effectful adapters use separate
execution paths. Model-generated plans are never authorization decisions.
Physical safety and fencing must be enforced by external controllers.
"""
from __future__ import annotations

import json
import math
from threading import Event, Thread
from time import monotonic
from collections.abc import Callable, Mapping
from typing import Any

from ..core.canonical_json import content_hash
from ..harness.execution_bridge import AuthorizedExecutionBridge, ExecutionTicket
from ..core.contracts import DomainError
from ..core.ids import uid
from ..planning.contracts import TaskPlan
from ..planning.validator import PlanValidator
from ..storage.sqlite.database import SqliteStore
from ..tools.contracts import Effect
from ..tools.registry import PluggableToolRegistry
from .workflow_store import WorkflowSnapshot, WorkflowStore


# Trusted application code must install these policies, not the LLM.
# A True verdict is based on an actual tool result and the expected condition.
EvidencePolicy = Callable[[Mapping[str, Any], Mapping[str, Any]], bool]


class WorkflowOrchestrator:
    """Coordinates a validated plan with fenced, durable subtask invocations.

    ``run`` persists the plan before any tool call. ``resume`` never repeats a
    recorded in-flight invocation after interruption. Side-effecting adapters
    require an independently configured execution grant and device-side fence.
    """

    def __init__(self, store: SqliteStore, registry: PluggableToolRegistry,
                 evidence_policies: Mapping[str, EvidencePolicy] | None = None,
                 execution_bridge: AuthorizedExecutionBridge | None = None,
                 require_agent_feedback: bool = False) -> None:
        self.store = store
        self.workflows = WorkflowStore(store)
        self.registry = registry
        self.validator = PlanValidator(registry)
        if execution_bridge is not None and execution_bridge.registry is not registry:
            raise DomainError("EXECUTION_BRIDGE_CATALOG_MISMATCH")
        self.execution_bridge = execution_bridge
        if type(require_agent_feedback) is not bool:
            raise DomainError("INVALID_AGENT_FEEDBACK_GATE")
        self.require_agent_feedback = require_agent_feedback
        self.evidence_policies = dict(evidence_policies or {})
        if any(not isinstance(k, str) or not callable(v)
               for k, v in self.evidence_policies.items()):
            raise DomainError("INVALID_EVIDENCE_POLICY")

    def catalog_hash(self) -> str:
        return content_hash(self.registry.catalog())

    def run(self, plan: TaskPlan, *, run_id: str | None = None,
            max_invocations: int = 20,
            policy_pin: tuple[str, str] | None = None,
            routing_manifest: dict[str, Any] | None = None) -> WorkflowSnapshot:
        """Persist a validated plan/policy pin BEFORE any subtask dispatch."""
        if not self.validator.is_validated(plan):
            raise DomainError("UNVALIDATED_TASK_PLAN")
        identifier = run_id or uid()
        with self.store.worker_session():
            self.workflows.create(identifier, plan, self.catalog_hash(), policy_pin=policy_pin,
                                  routing_manifest=routing_manifest)
            return self._drive(identifier, max_invocations)

    def resume(self, run_id: str, *, max_invocations: int = 20) -> WorkflowSnapshot:
        """Safely continue a workflow without redispatching ambiguous steps."""
        with self.store.worker_session():
            return self._drive(run_id, max_invocations)

    def _invoke_read_only_bounded(self, tool_id: str, arguments: dict[str, Any],
                                  timeout_seconds: float = 60.0) -> dict[str, Any]:
        """Bound caller wait and refresh the worker lease during slow model calls.

        A timed-out provider thread may continue: its result is never committed.
        The durable invocation stays DISPATCHING so restart requires review.
        """
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise DomainError("INVALID_TOOL_TIMEOUT")
        done = Event()
        outcome: dict[str, Any] = {}

        def worker() -> None:
            try:
                outcome["result"] = self.registry.dispatch_read_only(tool_id, arguments).output
            except Exception:
                outcome["error"] = "READ_ONLY_TOOL_FAILED"
            finally:
                done.set()

        started = monotonic()
        Thread(target=worker, name="agent-readonly-tool", daemon=True).start()
        while not done.wait(min(0.25, max(0.0, timeout_seconds - (monotonic() - started)))):
            self.store.heartbeat_worker()
            if monotonic() - started >= timeout_seconds:
                raise DomainError("READ_ONLY_TOOL_TIMEOUT")
        self.store.heartbeat_worker()
        # Completion just after the deadline must not be committed as success.
        # Event.wait(0.25) can otherwise tolerate a late but fast-returned call.
        if monotonic() - started >= timeout_seconds:
            raise DomainError("READ_ONLY_TOOL_TIMEOUT")
        if "error" in outcome:
            raise DomainError("READ_ONLY_TOOL_FAILED")
        return outcome["result"]

    def _invoke_effectful_bounded(self, ticket: ExecutionTicket,
                                  arguments: dict[str, Any]) -> dict[str, Any]:
        """Bound the wait, NOT execution duration or physical side effects.

        Provider threads may outlive the timeout. Callers MUST quarantine on
        failure or timeout and never auto-retry this invocation.
        """
        if self.execution_bridge is None:
            raise DomainError("EXECUTION_BRIDGE_NOT_BOUND")
        bridge = self.execution_bridge
        done = Event()
        outcome: dict[str, Any] = {}

        def worker() -> None:
            try:
                outcome["result"] = bridge.dispatch(ticket, arguments)
            except Exception:
                outcome["error"] = True
            finally:
                done.set()

        started = monotonic()
        Thread(target=worker, name="effectful-tool-adapter", daemon=True).start()
        while not done.wait(min(0.25, max(0.0, ticket.timeout_seconds - (monotonic() - started)))):
            self.store.heartbeat_worker()
            if monotonic() - started >= ticket.timeout_seconds:
                raise DomainError("EFFECTFUL_EXECUTION_TIMEOUT")
        self.store.heartbeat_worker()
        # A late reply cannot turn a timed-out physical action into success.
        if monotonic() - started >= ticket.timeout_seconds:
            raise DomainError("EFFECTFUL_EXECUTION_TIMEOUT")
        if outcome.get("error"):
            raise DomainError("EFFECTFUL_EXECUTION_UNKNOWN")
        return outcome["result"]

    def _request_stop_bounded(self, ticket: ExecutionTicket) -> str:
        """Stop acknowledgement is diagnostic; it does not release quarantine."""
        if self.execution_bridge is None:
            return "UNKNOWN"
        done = Event()
        result: dict[str, str] = {}
        bridge = self.execution_bridge

        def worker() -> None:
            try:
                result["ack"] = bridge.stop(ticket)
            except Exception:
                result["ack"] = "UNKNOWN"
            finally:
                done.set()

        Thread(target=worker, name="effectful-tool-stop", daemon=True).start()
        return result.get("ack", "UNKNOWN") if done.wait(1.0) else "UNKNOWN"

    def _drive(self, run_id: str, max_invocations: int) -> WorkflowSnapshot:
        if type(max_invocations) is not int or not 1 <= max_invocations <= 20:
            raise DomainError("INVALID_WORKFLOW_BUDGET")
        plan_data, catalog_hash = self.workflows.load_plan(run_id)
        policy_pin = self.workflows.policy_pin(run_id)
        runtime_binding = None
        if policy_pin is not None:
            # Enforcement at the trusted dispatch boundary prevents a caller
            # from bypassing read-only research isolation via direct resume().
            from ..governance.runtime_policy import RuntimePolicyActivator
            runtime_binding = RuntimePolicyActivator(self.store).read(policy_pin[0])
            if runtime_binding.manifest_hash != policy_pin[1]:
                raise DomainError("RUNTIME_POLICY_PIN_MISMATCH")
            if any(self.registry.resolve(step["tool_id"]).effect != Effect.READ_ONLY
                   for step in plan_data["subtasks"]):
                raise DomainError("RUNTIME_POLICY_PHYSICAL_DISPATCH_DENIED")
        if self.workflows.recover_inflight(run_id):
            return self.workflows.snapshot(run_id)
        existing = self.workflows.snapshot(run_id)
        if existing.status in {"SUCCEEDED", "BLOCKED", "HUMAN_REVIEW"}:
            return existing
        # The exact declared catalog is pinned; rebinding a handler does not
        # change its description, but replacing tool contracts requires a new run.
        if catalog_hash != self.catalog_hash():
            self.workflows.block(run_id, "TOOL_CATALOG_CHANGED")
            return self.workflows.snapshot(run_id)
        try:
            plan = self.validator.parse(
                json.dumps(plan_data, ensure_ascii=False),
                plan_data["goal"],
            )
        except (DomainError, TypeError, ValueError, KeyError):
            self.workflows.block(run_id, "STORED_PLAN_INVALID")
            return self.workflows.snapshot(run_id)

        count = 0
        while count < max_invocations:
            snapshot = self.workflows.snapshot(run_id)
            if snapshot.status in {"SUCCEEDED", "BLOCKED", "HUMAN_REVIEW"}:
                return snapshot
            completed = {key for key, status in snapshot.subtask_states.items()
                         if status == "SUCCEEDED"}
            # Optional dev5 semantic feedback barrier. It prevents another
            # workflow-resume caller from bypassing this agent's assessment turn.
            if self.require_agent_feedback and completed:
                turns = self.workflows.agent_turns(run_id)
                latest = {turn["subtask_id"]: turn for turn in turns}
                if any(latest.get(key, {}).get("action") != "CONTINUE"
                       for key in completed):
                    self.workflows.wait(run_id, "WAITING_ASSESSMENT",
                                        "AGENT_FEEDBACK_REQUIRED")
                    return self.workflows.snapshot(run_id)
            if len(completed) == len(plan.subtasks):
                self.workflows.complete(run_id)
                return self.workflows.snapshot(run_id)
            ready = plan.ready(completed)
            if not ready:
                self.workflows.block(run_id, "WORKFLOW_DEPENDENCY_STALLED")
                return self.workflows.snapshot(run_id)
            step = ready[0]
            if (runtime_binding is not None
                and runtime_binding.max_tool_calls is not None
                and self.workflows.dispatched_count(self.workflows.lineage_root(run_id))
                    >= runtime_binding.max_tool_calls):
                self.workflows.wait(run_id, "WAITING_POLICY", "RUNTIME_POLICY_TOOL_BUDGET_REACHED")
                return self.workflows.snapshot(run_id)
            spec = self.registry.resolve(step.tool_id)
            if spec.effect == Effect.PHYSICAL_ACTION:
                if self.execution_bridge is None or not self.execution_bridge.is_bound(spec.tool_id):
                    self.workflows.wait(run_id, "WAITING_EXECUTION_BRIDGE",
                                        "EFFECTFUL_TOOL_REQUIRES_AUTHORIZED_ADAPTER")
                    return self.workflows.snapshot(run_id)
            elif not self.registry.is_bound(spec.tool_id):
                self.workflows.wait(run_id, "WAITING_TOOL", "TOOL_NOT_BOUND")
                return self.workflows.snapshot(run_id)
            policy = self.evidence_policies.get(step.success_condition)
            if policy is None:
                self.workflows.wait(run_id, "WAITING_VERIFIER", "VERIFIER_NOT_BOUND")
                return self.workflows.snapshot(run_id)

            invocation_id = uid()
            arguments = step.arguments()
            if spec.effect == Effect.PHYSICAL_ACTION:
                assert self.execution_bridge is not None
                try:
                    ticket = self.execution_bridge.authorize(
                        spec=spec, run_id=run_id, subtask_id=step.subtask_id,
                        invocation_id=invocation_id, success_condition=step.success_condition,
                        arguments=arguments, fencing_token=self.store.active_fencing_token(),
                    )
                except DomainError:
                    self.workflows.block(run_id, "EXECUTION_AUTHORIZATION_DENIED")
                    return self.workflows.snapshot(run_id)
                try:
                    self.workflows.begin(run_id, step.subtask_id, invocation_id, {
                        "resource": ticket.resource, "tool_id": ticket.tool_id,
                        "arguments_hash": ticket.arguments_hash,
                        "fencing_token": ticket.fencing_token,
                    })
                except DomainError as exc:
                    if exc.code in {"RESOURCE_QUARANTINED", "RESOURCE_IN_FLIGHT",
                                    "RESOURCE_IN_FLIGHT_OR_QUARANTINED"}:
                        self.workflows.wait(run_id, "WAITING_EXECUTION_BRIDGE", exc.code)
                        return self.workflows.snapshot(run_id)
                    raise
                # Intent and grant are durable BEFORE external dispatch.
                try:
                    evidence = self._invoke_effectful_bounded(ticket, arguments)
                except DomainError as exc:
                    if exc.code in {"STALE_WORKER_FENCE", "WORKER_FENCE_REQUIRED"}:
                        raise
                    stop_ack = self._request_stop_bounded(ticket)
                    self.workflows.quarantine_effect(
                        run_id, step.subtask_id, invocation_id, exc.code, stop_ack,
                    )
                    return self.workflows.snapshot(run_id)
                try:
                    # Must check independently observed state, not provider success.
                    verified = policy(arguments, evidence) is True
                except Exception:
                    verified = False
                if not verified:
                    stop_ack = self._request_stop_bounded(ticket)
                    self.workflows.quarantine_effect(
                        run_id, step.subtask_id, invocation_id,
                        "EFFECTFUL_EVIDENCE_REJECTED", stop_ack,
                    )
                    return self.workflows.snapshot(run_id)
                self.workflows.finish(run_id, step.subtask_id, invocation_id,
                                      evidence, True, None)
            else:
                self.workflows.begin(run_id, step.subtask_id, invocation_id)
                # Model/network calls occur outside SQLite transactions.
                try:
                    tool_output = self._invoke_read_only_bounded(spec.tool_id, arguments)
                except DomainError as exc:
                    if exc.code in {"STALE_WORKER_FENCE", "WORKER_FENCE_REQUIRED"}:
                        raise
                    if exc.code == "READ_ONLY_TOOL_TIMEOUT":
                        self.workflows.recover_inflight(run_id)
                        return self.workflows.snapshot(run_id)
                    self.workflows.finish(run_id, step.subtask_id, invocation_id,
                                          None, False, "READ_ONLY_TOOL_FAILED")
                    return self.workflows.snapshot(run_id)
                try:
                    verified = policy(arguments, tool_output) is True
                except Exception:
                    verified = False
                self.workflows.finish(
                    run_id, step.subtask_id, invocation_id, tool_output,
                    verified, None if verified else "SUBTASK_VERIFICATION_REJECTED",
                )
                if not verified:
                    return self.workflows.snapshot(run_id)
            count += 1
        return self.workflows.snapshot(run_id)
