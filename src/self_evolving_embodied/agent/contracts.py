"""Single-LLM-agent decision contracts; no new agents or device authority."""
from __future__ import annotations

from dataclasses import dataclass
import json
from enum import StrEnum
from typing import Any, Mapping, Protocol

from ..core.contracts import DomainError
from ..planning.contracts import TaskPlan
from ..tools.contracts import _bounded_json


class AgentAction(StrEnum):
    CONTINUE = "CONTINUE"
    REPLAN = "REPLAN"
    WAIT_FOR_OBSERVATION = "WAIT_FOR_OBSERVATION"
    STOP = "STOP"


@dataclass(frozen=True)
class AgentDecision:
    action: AgentAction
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.action, AgentAction):
            raise DomainError("INVALID_AGENT_DECISION")
        if not isinstance(self.reason, str) or not 1 <= len(self.reason.strip()) <= 400:
            raise DomainError("INVALID_AGENT_DECISION")


@dataclass(frozen=True)
class BrainContext:
    """Bounded task/report context only. Never pass raw sensor payloads."""
    goal: str
    subtask_id: str
    tool_id: str
    execution_status: str
    tool_report: Mapping[str, Any]
    assessment: Mapping[str, Any] | None
    plan_revision: int

    def to_dict(self) -> dict[str, Any]:
        report = dict(self.tool_report)
        report_json = json.dumps(report, ensure_ascii=False, default=str, sort_keys=True)
        if len(report_json.encode("utf-8")) > 3000:
            report = {"summary_truncated": True, "preview": report_json[:2000]}
        assessment = dict(self.assessment) if self.assessment is not None else None
        if assessment is not None and len(json.dumps(assessment, ensure_ascii=False).encode("utf-8")) > 2500:
            assessment = {"outcome": assessment.get("outcome", "UNKNOWN"),
                          "assessor_id": assessment.get("assessor_id"),
                          "reason": str(assessment.get("reason", ""))[:800],
                          "summary_truncated": True}
        data = {"goal": self.goal, "subtask_id": self.subtask_id,
                "tool_id": self.tool_id, "execution_status": self.execution_status,
                "tool_report": report,
                "assessment": assessment,
                "plan_revision": self.plan_revision}
        return _bounded_json(data)


class SingleAgentBrain(Protocol):
    def plan(self, goal: str) -> TaskPlan: ...
    def decide(self, context: BrainContext) -> AgentDecision: ...
    def replan(self, goal: str, context: BrainContext) -> TaskPlan: ...


@dataclass(frozen=True)
class AssessmentRoute:
    """Trusted host routing. The LLM cannot install or change this mapping."""
    tool_id: str
    assessor_id: str

    def __post_init__(self) -> None:
        if (not isinstance(self.tool_id, str) or not self.tool_id.strip()
            or not isinstance(self.assessor_id, str) or not self.assessor_id.strip()):
            raise DomainError("INVALID_ASSESSMENT_ROUTE")


@dataclass(frozen=True)
class AgentLoopReport:
    root_run_id: str
    active_run_id: str
    status: str
    workflow_status: str
    completed_turns: int
    replans: int
    last_outcome: str
    reason: str | None = None
