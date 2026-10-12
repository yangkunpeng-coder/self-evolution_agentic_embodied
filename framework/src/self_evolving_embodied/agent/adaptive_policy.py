"""Pinned, trusted adaptive routing for controlled READ_ONLY research.

This module never registers plugins or grants tool authority. A human-approved
runtime PolicyProfile enables the behavior; trusted application configuration
limits which tools may substitute for one another. The exact configuration is
hashed into the workflow journal so resumed tasks cannot silently change it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..runtime.workflow_store import _digest
from ..core.contracts import DomainError
from ..governance.runtime_policy import RuntimePolicyBinding
from ..observations import ObservationReader, ObservationRequest
from ..planning.contracts import Subtask, TaskPlan
from ..planning.validator import PlanValidator
from ..toolboxes.assessment import AssessmentToolbox
from ..toolboxes.common import ObservationNeeds, unavailable_required, make_reader
from ..toolboxes.execution import ExecutionToolbox
from ..tools.contracts import Effect
from ..tools.registry import PluggableToolRegistry
from .contracts import AssessmentRoute


@dataclass(frozen=True)
class TrustedToolAlternative:
    """Host-reviewed semantic equivalence, never suggested by the LLM.

    Both IDs must expose equal typed arguments, capability, success conditions,
    and ToolKind. Equal schemas alone do not establish real equivalence; the
    trusted host is accountable for that declaration.
    """
    original_id: str
    preferred_id: str

    def __post_init__(self) -> None:
        if (not isinstance(self.original_id, str) or not isinstance(self.preferred_id, str)
                or not self.original_id or not self.preferred_id
                or self.original_id == self.preferred_id):
            raise DomainError("INVALID_TRUSTED_TOOL_ALTERNATIVE")


class AdaptivePolicyRouter:
    """Deterministic tool and assessor routing with provenance-only manifest.

    Modifying the original host configuration, plugin catalog or assessor
    registration changes the manifest hash, failing resume before a new action.
    """
    def __init__(self, registry: PluggableToolRegistry, assessments: AssessmentToolbox,
                 routes: Sequence[AssessmentRoute],
                 alternatives: Sequence[TrustedToolAlternative] = (),
                 execution: ExecutionToolbox | None = None,
                 observations: ObservationReader | None = None) -> None:
        if not isinstance(registry, PluggableToolRegistry) or not isinstance(assessments, AssessmentToolbox):
            raise DomainError("INVALID_ADAPTIVE_ROUTER")
        if execution is not None and execution.registry is not registry:
            raise DomainError("ADAPTIVE_ROUTER_REGISTRY_MISMATCH")
        if any(not isinstance(x, AssessmentRoute) for x in routes):
            raise DomainError("INVALID_ASSESSMENT_ROUTE")
        if any(not isinstance(x, TrustedToolAlternative) for x in alternatives):
            raise DomainError("INVALID_TRUSTED_TOOL_ALTERNATIVE")
        self.registry, self.assessments = registry, assessments
        self.execution, self.observations = execution, observations
        self.routes = {x.tool_id: x.assessor_id for x in routes}
        if len(self.routes) != len(routes):
            raise DomainError("DUPLICATE_ASSESSMENT_ROUTE")
        for tool_id, assessor_id in self.routes.items():
            registry.resolve(tool_id)
            assessments.resolve(assessor_id)
        self.alternatives = {x.original_id: x.preferred_id for x in alternatives}
        if len(self.alternatives) != len(alternatives):
            raise DomainError("DUPLICATE_TRUSTED_TOOL_ALTERNATIVE")
        for original_id, preferred_id in self.alternatives.items():
            original = registry.resolve(original_id)
            preferred = registry.resolve(preferred_id)
            if (original.effect != Effect.READ_ONLY or preferred.effect != Effect.READ_ONLY
                or original.capability != preferred.capability
                or original.argument_types != preferred.argument_types
                or original.success_conditions != preferred.success_conditions
                or original.kind != preferred.kind):
                raise DomainError("UNSAFE_TOOL_ALTERNATIVE")
            # Do not silently replace a routed tool with a tool assessed using
            # a different outcome specification.
            if self.routes.get(original_id) != self.routes.get(preferred_id):
                raise DomainError("TOOL_ALTERNATIVE_ASSESSOR_MISMATCH")

    @property
    def manifest_hash(self) -> str:
        return _digest(self.manifest())

    def manifest(self) -> dict:
        # Re-read public contracts so a mutation cannot hide behind a cached hash.
        return {
            "schema_version": 1,
            "tool_catalog": self.registry.catalog(),
            "assessors": self.assessments.catalog(),
            "bound_assessors": self.assessments.bound_ids(),
            "routes": [{"tool_id": x, "assessor_id": self.routes[x]}
                       for x in sorted(self.routes)],
            "approved_alternatives": [{"original_id": x, "preferred_id": self.alternatives[x]}
                                      for x in sorted(self.alternatives)],
            "execution_observation_needs": (self.execution.catalog() if self.execution is not None else None),
        }

    def rewrite_plan(self, plan: TaskPlan, binding: RuntimePolicyBinding | None) -> TaskPlan:
        profile = binding.get("TOOL_SELECTION") if binding else None
        if profile is None or not profile.settings["prefer_read_only_first"]:
            return plan
        steps = []
        for step in plan.subtasks:
            preferred_id = self.alternatives.get(step.tool_id)
            if preferred_id is not None and self.registry.is_bound(preferred_id):
                step = Subtask(step.subtask_id, preferred_id, step.arguments_json,
                               step.depends_on, step.success_condition)
            steps.append(step)
        rewritten = TaskPlan(plan.goal, tuple(steps), plan.planner_id)
        if not PlanValidator(self.registry).is_validated(rewritten):
            raise DomainError("ADAPTIVE_REWRITTEN_PLAN_INVALID")
        return rewritten

    def select_assessor(self, tool_id: str, binding: RuntimePolicyBinding | None) -> str | None:
        baseline = self.routes.get(tool_id)
        profile = binding.get("ASSESSMENT_ROUTING") if binding else None
        if baseline is None or profile is None or not profile.settings["prefer_independent_vision"]:
            return baseline
        capability = self.assessments.resolve(baseline).capability
        for spec in self.assessments.find_by_capability(capability):
            if (spec.method == "VISION" and spec.independent_from_execution
                and self.assessments.ready(
                    spec.assessor_id,
                    max_age_ms=(binding.max_observation_age_ms if binding else None))):
                return spec.assessor_id
        return baseline

    def pre_observation(self, plan: TaskPlan, binding: RuntimePolicyBinding | None) -> None:
        profile = binding.get("PLANNING_STRATEGY") if binding else None
        if profile is None or not profile.settings["require_pre_observation"]:
            return
        if self.execution is None or self.observations is None:
            raise DomainError("POLICY_PRE_OBSERVATION_PROVIDER_REQUIRED")
        acquisition = binding.get("OBSERVATION_ACQUISITION") if binding else None
        max_age = acquisition.settings["max_observation_age_ms"] if acquisition else None
        saw_required = False
        for step in plan.subtasks:
            needs = self.execution.resolve(step.tool_id).observation_needs
            if needs.required:
                saw_required = True
                tightened = ObservationNeeds(
                    required=tuple(ObservationRequest(req.key, min(req.max_age_ms, max_age)
                        if req.max_age_ms is not None and max_age is not None else
                        (max_age if max_age is not None else req.max_age_ms))
                        for req in needs.required),
                    optional=needs.optional,
                )
                reader = make_reader(self.observations, tightened)
                if unavailable_required(reader, tightened):
                    raise DomainError("POLICY_PRE_OBSERVATION_MISSING_OR_STALE")
        if not saw_required:
            raise DomainError("POLICY_PRE_OBSERVATION_UNDECLARED")
