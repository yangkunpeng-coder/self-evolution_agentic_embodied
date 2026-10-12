"""Simple built-in sensor rule assessor. An optional baseline, not ground truth."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ...core.contracts import DomainError
from ...observations import ObservationReader, ObservationRequest
from .contracts import (
    AssessmentRequest, AssessmentResult, ObservationRef, Outcome,
)


@dataclass(frozen=True)
class SensorFieldRule:
    """Trusted configuration of an explicit observation-field comparator."""

    observation: ObservationRequest
    field_path: tuple[str, ...]
    comparator: str
    expected: str | int | float | bool | None

    def __post_init__(self) -> None:
        if not isinstance(self.observation, ObservationRequest):
            raise DomainError("INVALID_ASSESSMENT_RULE")
        if not isinstance(self.field_path, tuple) or not self.field_path or len(self.field_path) > 8:
            raise DomainError("INVALID_ASSESSMENT_RULE")
        if any(not isinstance(field, str) or not field.isidentifier() or field.startswith("_")
               for field in self.field_path):
            raise DomainError("INVALID_ASSESSMENT_RULE")
        if self.comparator not in {"equals", "gte", "lte"}:
            raise DomainError("INVALID_ASSESSMENT_RULE")
        if type(self.expected) not in (str, int, float, bool, type(None)):
            raise DomainError("INVALID_ASSESSMENT_RULE")
        if self.comparator != "equals" and type(self.expected) not in (float, int):
            raise DomainError("INVALID_ASSESSMENT_RULE")


class SensorFieldRuleAssessor:
    """Reads one fresh sensor mapping, evaluates a preconfigured predicate."""

    def __init__(self, assessor_id: str, rule: SensorFieldRule) -> None:
        self.assessor_id = assessor_id
        self.rule = rule

    def assess(self, request: AssessmentRequest,
               observations: ObservationReader | None) -> AssessmentResult:
        if observations is None:
            return AssessmentResult(Outcome.UNKNOWN, self.assessor_id,
                                    "Observation source not configured")
        observation = observations.latest(self.rule.observation)
        if observation is None:
            return AssessmentResult(Outcome.UNKNOWN, self.assessor_id,
                                    "Observation missing or too old")
        pointer = ObservationRef.from_observation(observation)
        value: Any = observation.payload
        for field in self.rule.field_path:
            if not isinstance(value, Mapping) or field not in value:
                return AssessmentResult(Outcome.UNKNOWN, self.assessor_id,
                                        "Required observation field unavailable", (pointer,))
            value = value[field]
        expected = self.rule.expected
        if self.rule.comparator == "equals":
            match = type(value) is type(expected) and value == expected
        elif type(value) not in (int, float) or not isinstance(value, (int, float)):
            return AssessmentResult(Outcome.UNKNOWN, self.assessor_id,
                                    "Numeric observation required", (pointer,))
        elif self.rule.comparator == "gte":
            match = value >= expected
        else:
            match = value <= expected
        return AssessmentResult(
            Outcome.SUPPORTED if match else Outcome.NOT_SUPPORTED,
            self.assessor_id,
            "Configured sensor-field rule matched" if match else "Configured sensor-field rule did not match",
            (pointer,),
        )
