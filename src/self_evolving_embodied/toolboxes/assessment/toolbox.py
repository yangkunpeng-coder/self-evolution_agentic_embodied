"""Independent assessor catalog with an optional existing-Harness adapter.

Assessment is a fallible inference under partial observability. No result is
promoted into device authority or into a physical task completion fact.
"""
from __future__ import annotations

from collections.abc import Mapping
import math
from threading import Event, Thread
from time import monotonic
from typing import Any

from ...core.contracts import DomainError
from ...observations import ObservationReader
from ...tools.contracts import Effect, ToolKind, ToolSpec, _bounded_json
from ...tools.registry import PluggableToolRegistry
from ..common import make_reader, unavailable_required
from .freshness import FreshnessBoundReader
from .contracts import AssessmentRequest, AssessmentResult, AssessorSpec, Outcome, OutcomeAssessor


class AssessmentToolbox:
    """Separate registry; its plugins receive only declared observation streams."""

    def __init__(self, observations: ObservationReader | None = None) -> None:
        self._observations = observations
        self._specs: dict[str, AssessorSpec] = {}
        self._handlers: dict[str, OutcomeAssessor] = {}

    def register(self, spec: AssessorSpec, handler: OutcomeAssessor | None = None) -> None:
        if not isinstance(spec, AssessorSpec):
            raise DomainError("INVALID_ASSESSOR_SPEC")
        if spec.assessor_id in self._specs:
            raise DomainError("ASSESSOR_ALREADY_REGISTERED")
        if handler is not None and not callable(getattr(handler, "assess", None)):
            raise DomainError("INVALID_ASSESSOR_HANDLER")
        self._specs[spec.assessor_id] = spec
        if handler is not None:
            self._handlers[spec.assessor_id] = handler

    def bind(self, assessor_id: str, handler: OutcomeAssessor) -> None:
        self.resolve(assessor_id)
        if not callable(getattr(handler, "assess", None)):
            raise DomainError("INVALID_ASSESSOR_HANDLER")
        self._handlers[assessor_id] = handler

    def resolve(self, assessor_id: str) -> AssessorSpec:
        try:
            return self._specs[assessor_id]
        except (KeyError, TypeError):
            raise DomainError("UNKNOWN_ASSESSOR") from None

    def catalog(self) -> list[dict[str, object]]:
        return [self._specs[k].public_description() for k in sorted(self._specs)]

    def find_by_capability(self, capability: str) -> tuple[AssessorSpec, ...]:
        if not isinstance(capability, str) or not capability.strip():
            raise DomainError("INVALID_ASSESSOR_CAPABILITY")
        return tuple(spec for spec in (self._specs[k] for k in sorted(self._specs))
                     if spec.capability == capability)

    def bound_ids(self) -> list[str]:
        return sorted(self._handlers)

    def ready(self, assessor_id: str, *, max_age_ms: int | None = None) -> bool:
        """Only positive declared observation evidence makes a vision route eligible."""
        spec = self.resolve(assessor_id)
        if assessor_id not in self._handlers:
            return False
        if spec.method == "VISION" and not spec.observation_needs.required:
            return False
        if self._observations is None:
            return not spec.observation_needs.required
        try:
            reader = make_reader(self._observations, spec.observation_needs)
            if reader is not None and max_age_ms is not None:
                reader = FreshnessBoundReader(reader, max_age_ms)
            return not unavailable_required(reader, spec.observation_needs)
        except (DomainError, ValueError, RuntimeError, LookupError):
            return False

    def assess(self, assessor_id: str, request: AssessmentRequest,
               *, max_age_ms: int | None = None,
               timeout_seconds: float = 60.0) -> AssessmentResult:
        """Trusted low-level invocation; production workflows should use Harness.

        Insufficient required sensor observations yield UNKNOWN (not failure).
        Unexpected provider exceptions never become an affirmative verdict.
        """
        spec = self.resolve(assessor_id)
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 300):
            raise DomainError("INVALID_ASSESSMENT_TIMEOUT")
        if not isinstance(request, AssessmentRequest):
            raise DomainError("INVALID_ASSESSMENT_REQUEST")
        handler = self._handlers.get(assessor_id)
        if handler is None:
            raise DomainError("ASSESSOR_NOT_BOUND")
        needs = spec.observation_needs
        reader = make_reader(self._observations, needs) if self._observations is not None else None
        if max_age_ms is not None:
            if type(max_age_ms) is not int or not 1 <= max_age_ms <= 10000:
                raise DomainError("INVALID_ASSESSMENT_FRESHNESS_LIMIT")
            if reader is not None:
                reader = FreshnessBoundReader(reader, max_age_ms)
        unavailable = unavailable_required(reader, needs)
        if unavailable:
            names = ", ".join(f"{key.source_id}/{key.modality}" for key in unavailable)
            return AssessmentResult(Outcome.UNKNOWN, assessor_id,
                                    f"Required observations missing or stale: {names}")
        # Bound the caller wait. As with read-only execution tools, Python
        # cannot terminate a timed-out provider thread; discard any late result.
        done = Event()
        reply: dict[str, Any] = {}

        def worker() -> None:
            try:
                reply["result"] = handler.assess(request, reader)
            except Exception:
                reply["error"] = True
            finally:
                done.set()

        started = monotonic()
        Thread(target=worker, name="agent-assessment-tool", daemon=True).start()
        if not done.wait(timeout_seconds) or monotonic() - started >= timeout_seconds:
            raise DomainError("ASSESSMENT_PROVIDER_TIMEOUT")
        if reply.get("error"):
            raise DomainError("ASSESSMENT_PROVIDER_FAILED")
        result = reply.get("result")
        if not isinstance(result, AssessmentResult) or result.assessor_id != assessor_id:
            raise DomainError("INVALID_ASSESSMENT_RESULT")
        # Keep audit/event payloads compact; the original sensor data stays in Queue.
        _bounded_json(result.to_dict())
        return result

    def mount_as_harness_tool(self, assessor_id: str,
                              registry: PluggableToolRegistry,
                              *, tool_id: str | None = None) -> ToolSpec:
        """Expose assessor through the *existing* event-sourced workflow path.

        This records an assessment step via WorkflowOrchestrator if used in a
        validated TaskPlan with assessment_recorded_policy. Completion of the
        step means 'assessment generated', not 'robot goal achieved'.
        """
        spec = self.resolve(assessor_id)
        if assessor_id not in self._handlers:
            raise DomainError("ASSESSOR_NOT_BOUND")
        if not isinstance(registry, PluggableToolRegistry):
            raise DomainError("INVALID_TOOL_REGISTRY")
        effective_id = tool_id or f"assessment.{assessor_id}"
        tool_spec = ToolSpec(
            tool_id=effective_id,
            capability=spec.capability,
            kind=ToolKind.CUSTOM,
            description=f"Assessment only: {spec.description}"[:300],
            argument_types=(("request", "object"),),
            success_conditions=("assessment_recorded",),
            version=spec.version,
            effect=Effect.READ_ONLY,
        )
        registry.register(tool_spec, _AssessmentHarnessHandler(self, assessor_id))
        return tool_spec


class _AssessmentHarnessHandler:
    def __init__(self, toolbox: AssessmentToolbox, assessor_id: str) -> None:
        self._toolbox = toolbox
        self._assessor_id = assessor_id

    def invoke(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        if not isinstance(arguments, Mapping) or set(arguments) != {"request"}:
            raise DomainError("INVALID_ASSESSMENT_REQUEST")
        request = AssessmentRequest.from_dict(arguments["request"])
        return self._toolbox.assess(self._assessor_id, request).to_dict()


def assessment_recorded_policy(_arguments: Mapping[str, Any],
                               output: Mapping[str, Any]) -> bool:
    """Only validates the *record*, never endorses the physical task outcome."""
    return (isinstance(output, Mapping)
            and output.get("outcome") in {value.value for value in Outcome}
            and isinstance(output.get("assessor_id"), str)
            and isinstance(output.get("reason"), str)
            and isinstance(output.get("observation_refs"), list))
