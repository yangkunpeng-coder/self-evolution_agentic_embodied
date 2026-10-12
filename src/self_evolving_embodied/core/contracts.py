"""Restricted immutable MVP contracts. External untrusted JSON is validated explicitly."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol


class DomainError(Exception):
    """A controlled, safe-to-log domain failure."""
    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        super().__init__(f"{code}: {message}" if message else code)


class Status(StrEnum):
    CREATED = "CREATED"
    READY = "READY"
    RUNNING = "RUNNING"
    WAITING_EXECUTION = "WAITING_EXECUTION"
    VERIFYING = "VERIFYING"
    RECOVERING = "RECOVERING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    HUMAN_REVIEW = "HUMAN_REVIEW"


class Verdict(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True)
class Postcondition:
    condition_id: str
    field: str
    expected: str | int | bool | None = None
    kind: str = "equals"
    source_requirement: str = "fresh_observation"

    def __post_init__(self) -> None:
        if not self.condition_id or not self.field or self.field.startswith("_"):
            raise DomainError("INVALID_CONDITION")
        if self.kind not in {"equals", "exists"}:
            raise DomainError("UNSUPPORTED_CONDITION")
        if type(self.expected) not in (str, int, bool, type(None)):
            raise DomainError("INVALID_CONDITION_VALUE")
        if self.source_requirement != "fresh_observation":
            raise DomainError("UNSUPPORTED_EVIDENCE_SOURCE")
        if not all(p.isidentifier() and not p.startswith("_") for p in self.field.split(".")):
            raise DomainError("INVALID_FIELD_PATH")


@dataclass(frozen=True)
class TaskContract:
    task_id: str
    goal: Postcondition
    allowed_capabilities: tuple[str, ...]
    release_id: str = "R0-FAKE"
    max_steps: int = 1
    max_retries: int = 0
    wall_deadline_ms: int = 30_000

    def __post_init__(self) -> None:
        if not self.task_id or not self.allowed_capabilities:
            raise DomainError("INVALID_TASK")
        if self.max_steps < 1 or self.max_retries < 0 or self.wall_deadline_ms < 1:
            raise DomainError("INVALID_BUDGET")


@dataclass(frozen=True)
class PlanStep:
    step_id: str
    tool_id: str
    capability_id: str
    arguments: dict[str, Any]
    postcondition: Postcondition
    release_id: str
    expected_state_version: int
    based_on_snapshot_id: str = ""


@dataclass(frozen=True)
class ToolDescriptor:
    tool_id: str
    capability_id: str
    arguments_allowlist: tuple[str, ...]
    release_id: str = "R0-FAKE"
    idempotent: bool = False
    resource: str = "fake-arm"


@dataclass(frozen=True)
class AuthorizedInvocation:
    task_id: str
    step_id: str
    invocation_id: str
    release_id: str
    tool_id: str
    state_version: int
    arguments_hash: str
    resource: str
    fencing_token: int = 0  # R03 monotonic epoch; external adapters must enforce it


@dataclass(frozen=True)
class Observation:
    observation_id: str
    task_id: str
    release_id: str
    invocation_id: str | None
    observed_at: str
    state: dict[str, Any]
    ttl_ms: int = 10_000


@dataclass(frozen=True)
class ProviderResponse:
    invocation_id: str
    status: str
    side_effect_observed: str


@dataclass(frozen=True)
class EvidenceBundle:
    evidence_id: str
    task_id: str
    step_id: str | None
    invocation_id: str
    release_id: str
    before: Observation | None
    after: Observation | None
    scope: str


@dataclass(frozen=True)
class VerificationVerdict:
    verdict_id: str
    task_id: str
    step_id: str | None
    scope: str
    release_id: str
    evidence_hash: str
    condition_id: str
    result: Verdict
    verifier_id: str = "mvp-independent-verifier-v1"


@dataclass(frozen=True)
class TaskOutcome:
    task_id: str
    status: Status
    release_id: str
    state_version: int
    invocation_count: int
    failure_code: str | None = None


class Planner(Protocol):
    def plan(self, task: TaskContract, state_version: int, context: object | None = None) -> PlanStep: ...


class ExecutionAdapter(Protocol):
    def observe(self, task_id: str, release_id: str, invocation_id: str | None) -> Observation | None: ...
    def invoke(self, ticket: AuthorizedInvocation, arguments: dict[str, Any]) -> ProviderResponse: ...
    def request_stop(self, invocation_id: str) -> str: ...
