"""Outcome assessment contracts; an assessment is not environment ground truth."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping, Protocol

from ...core.contracts import DomainError
from ...observations import Observation, ObservationKey, ObservationReader
from ...tools.contracts import _bounded_json
from ..common import ObservationNeeds

_IDENT = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


class Outcome(StrEnum):
    SUPPORTED = "SUPPORTED"
    NOT_SUPPORTED = "NOT_SUPPORTED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class AssessorSpec:
    assessor_id: str
    capability: str
    description: str
    observation_needs: ObservationNeeds = ObservationNeeds()
    version: str = "1.0.0"
    method: str = "GENERAL"
    independent_from_execution: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.assessor_id, str) or not _IDENT.fullmatch(self.assessor_id):
            raise DomainError("INVALID_ASSESSOR_ID")
        if not isinstance(self.capability, str) or not _IDENT.fullmatch(self.capability):
            raise DomainError("INVALID_ASSESSOR_CAPABILITY")
        if not isinstance(self.description, str) or not 1 <= len(self.description.strip()) <= 300:
            raise DomainError("INVALID_ASSESSOR_DESCRIPTION")
        if not isinstance(self.observation_needs, ObservationNeeds):
            raise DomainError("INVALID_OBSERVATION_NEEDS")
        if not isinstance(self.version, str) or not 1 <= len(self.version) <= 40:
            raise DomainError("INVALID_ASSESSOR_VERSION")
        if not isinstance(self.method, str) or self.method not in {"GENERAL", "VISION", "RULE", "LLM", "TEMPORAL", "CUSTOM"}:
            raise DomainError("INVALID_ASSESSOR_METHOD")
        if type(self.independent_from_execution) is not bool:
            raise DomainError("INVALID_ASSESSOR_INDEPENDENCE")

    def public_description(self) -> dict[str, object]:
        return {"assessor_id": self.assessor_id, "capability": self.capability,
                "description": self.description, "version": self.version,
                "method": self.method, "independent_from_execution": self.independent_from_execution,
                "observation_needs": self.observation_needs.public_description()}


@dataclass(frozen=True)
class AssessmentRequest:
    """Structured goal, tool execution status and bounded report summary.

    execution_status is reported operational state, NOT semantic task success.
    Raw images, LiDAR data, or ROS messages must not be passed in tool_report.
    """

    subtask_id: str
    goal: str
    execution_status: str
    tool_report: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.subtask_id, str) or not 1 <= len(self.subtask_id) <= 128:
            raise DomainError("INVALID_ASSESSMENT_REQUEST")
        if not isinstance(self.goal, str) or not 1 <= len(self.goal.strip()) <= 1000:
            raise DomainError("INVALID_ASSESSMENT_REQUEST")
        if not isinstance(self.execution_status, str) or not 1 <= len(self.execution_status) <= 64:
            raise DomainError("INVALID_ASSESSMENT_REQUEST")
        if not isinstance(self.tool_report, Mapping):
            raise DomainError("INVALID_ASSESSMENT_REQUEST")
        _bounded_json(dict(self.tool_report))

    @classmethod
    def from_dict(cls, data: object) -> "AssessmentRequest":
        if not isinstance(data, dict) or set(data) != {
            "subtask_id", "goal", "execution_status", "tool_report"
        }:
            raise DomainError("INVALID_ASSESSMENT_REQUEST")
        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        return {"subtask_id": self.subtask_id, "goal": self.goal,
                "execution_status": self.execution_status,
                "tool_report": _bounded_json(dict(self.tool_report))}


@dataclass(frozen=True)
class ObservationRef:
    source_id: str
    modality: str
    sequence_id: int
    received_monotonic_ns: int

    @classmethod
    def from_observation(cls, obs: Observation) -> "ObservationRef":
        return cls(obs.key.source_id, obs.key.modality, obs.sequence_id,
                   obs.received_monotonic_ns)

    def to_dict(self) -> dict[str, object]:
        return {"source_id": self.source_id, "modality": self.modality,
                "sequence_id": self.sequence_id,
                "received_monotonic_ns": self.received_monotonic_ns}


@dataclass(frozen=True)
class AssessmentResult:
    outcome: Outcome
    assessor_id: str
    reason: str
    observation_refs: tuple[ObservationRef, ...] = ()
    confidence: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, Outcome):
            raise DomainError("INVALID_ASSESSMENT_RESULT")
        if not isinstance(self.assessor_id, str) or not _IDENT.fullmatch(self.assessor_id):
            raise DomainError("INVALID_ASSESSMENT_RESULT")
        if not isinstance(self.reason, str) or not 1 <= len(self.reason.strip()) <= 1000:
            raise DomainError("INVALID_ASSESSMENT_RESULT")
        if not isinstance(self.observation_refs, tuple) or len(self.observation_refs) > 64 or any(
            not isinstance(ref, ObservationRef) for ref in self.observation_refs
        ):
            raise DomainError("INVALID_ASSESSMENT_RESULT")
        if self.confidence is not None and (
            type(self.confidence) not in (float, int)
            or not math.isfinite(self.confidence) or not 0 <= self.confidence <= 1
        ):
            raise DomainError("INVALID_ASSESSMENT_CONFIDENCE")

    def to_dict(self) -> dict[str, Any]:
        return {"outcome": self.outcome.value, "assessor_id": self.assessor_id,
                "reason": self.reason,
                "observation_refs": [ref.to_dict() for ref in self.observation_refs],
                "confidence": self.confidence}


class OutcomeAssessor(Protocol):
    """Trusted assessor implementation; may use VLM, LLM, rules, or custom code."""

    def assess(self, request: AssessmentRequest,
               observations: ObservationReader | None) -> AssessmentResult: ...
