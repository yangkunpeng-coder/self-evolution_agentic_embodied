"""Trusted, opt-in bridge for externally effectful tool adapters.

A registry entry or an LLM plan is not authority to perform physical effects.
Application code must separately bind an adapter AND grant a bounded capability.
The adapter must enforce the fencing epoch at the execution boundary. This
protocol alone cannot prove real-device safety or stop a running physical action.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any, Mapping, Protocol

from ..core.contracts import DomainError
from ..tools.contracts import Effect, ToolSpec, _bounded_json, check_arguments
from ..tools.registry import PluggableToolRegistry


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False,
                         allow_nan=False, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ExecutionGrant:
    """Trusted application configuration, never constructed from LLM output."""

    tool_id: str
    tool_version: str
    resource: str
    allowed_conditions: tuple[str, ...]
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if (not isinstance(self.tool_id, str) or not self.tool_id.strip()
            or not isinstance(self.tool_version, str) or not self.tool_version.strip()
            or not isinstance(self.resource, str) or not 1 <= len(self.resource) <= 128
            or not isinstance(self.allowed_conditions, tuple) or not self.allowed_conditions
            or any(not isinstance(c, str) or not c.strip() for c in self.allowed_conditions)
            or len(set(self.allowed_conditions)) != len(self.allowed_conditions)
            or isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not 0.1 <= self.timeout_seconds <= 300.0):
            raise DomainError("INVALID_EXECUTION_GRANT")


@dataclass(frozen=True)
class ExecutionTicket:
    run_id: str
    subtask_id: str
    invocation_id: str
    tool_id: str
    tool_version: str
    capability: str
    success_condition: str
    resource: str
    arguments_hash: str
    fencing_token: int
    timeout_seconds: float


class EffectfulToolAdapter(Protocol):
    """External adapter implemented by trusted integration code.

    enforce_fencing MUST be implemented at the simulator/controller boundary.
    observe MUST inspect independent environment state, not merely echo invoke.
    request_stop acknowledgement is diagnostic, not proof of safe stoppage.
    """

    def enforce_fencing(self, ticket: ExecutionTicket) -> bool: ...
    def observe(self, ticket: ExecutionTicket) -> Mapping[str, Any]: ...
    def invoke(self, ticket: ExecutionTicket, arguments: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def request_stop(self, ticket: ExecutionTicket) -> str: ...


class AuthorizedExecutionBridge:
    """An independent capability gate for effectful model/controller tools."""

    def __init__(self, registry: PluggableToolRegistry) -> None:
        self.registry = registry
        self._adapters: dict[str, EffectfulToolAdapter] = {}
        self._grants: dict[str, ExecutionGrant] = {}

    def bind(self, grant: ExecutionGrant, adapter: EffectfulToolAdapter) -> None:
        spec = self.registry.resolve(grant.tool_id)
        if spec.effect != Effect.PHYSICAL_ACTION or grant.tool_version != spec.version:
            raise DomainError("EXECUTION_GRANT_MISMATCH")
        if not set(grant.allowed_conditions).issubset(spec.success_conditions):
            raise DomainError("EXECUTION_CONDITION_NOT_ALLOWED")
        if not all(callable(getattr(adapter, method, None)) for method in (
            "enforce_fencing", "observe", "invoke", "request_stop"
        )):
            raise DomainError("INVALID_EXECUTION_ADAPTER")
        if grant.tool_id in self._grants:
            raise DomainError("EXECUTION_GRANT_ALREADY_BOUND")
        self._grants[grant.tool_id] = grant
        self._adapters[grant.tool_id] = adapter

    def is_bound(self, tool_id: str) -> bool:
        return tool_id in self._grants and tool_id in self._adapters

    def authorize(self, *, spec: ToolSpec, run_id: str, subtask_id: str,
                  invocation_id: str, success_condition: str,
                  arguments: dict[str, Any], fencing_token: int) -> ExecutionTicket:
        # Fresh resolution prevents caller-provided ToolSpec from widening authority.
        actual = self.registry.resolve(spec.tool_id)
        if actual != spec or spec.effect != Effect.PHYSICAL_ACTION:
            raise DomainError("EXECUTION_TOOL_MISMATCH")
        grant = self._grants.get(spec.tool_id)
        if grant is None or spec.tool_id not in self._adapters:
            raise DomainError("EXECUTION_BRIDGE_NOT_BOUND")
        if (grant.tool_version != spec.version
            or success_condition not in grant.allowed_conditions):
            raise DomainError("EXECUTION_NOT_AUTHORIZED")
        if type(fencing_token) is not int or fencing_token < 1:
            raise DomainError("WORKER_FENCE_REQUIRED")
        frozen = check_arguments(spec, arguments)
        return ExecutionTicket(
            run_id, subtask_id, invocation_id, spec.tool_id, spec.version,
            spec.capability, success_condition, grant.resource, _digest(frozen),
            fencing_token, grant.timeout_seconds,
        )

    def dispatch(self, ticket: ExecutionTicket, arguments: dict[str, Any]) -> dict[str, Any]:
        """External call; caller MUST have persisted an intent before entering here."""
        spec = self.registry.resolve(ticket.tool_id)
        grant = self._grants.get(ticket.tool_id)
        adapter = self._adapters.get(ticket.tool_id)
        if (grant is None or adapter is None or spec.effect != Effect.PHYSICAL_ACTION
            or grant.resource != ticket.resource or grant.tool_version != ticket.tool_version
            or ticket.success_condition not in grant.allowed_conditions):
            raise DomainError("EXECUTION_GRANT_CHANGED")
        checked = check_arguments(spec, arguments)
        if _digest(checked) != ticket.arguments_hash:
            raise DomainError("AUTHORIZED_ARGUMENTS_CHANGED")
        # Device-side fencing cannot be replaced by a SQLite-side lease check.
        if adapter.enforce_fencing(ticket) is not True:
            raise DomainError("DEVICE_FENCING_REJECTED")
        before = adapter.observe(ticket)
        if not isinstance(before, Mapping):
            raise DomainError("EXECUTION_PRE_OBSERVATION_MISSING")
        frozen_before = _bounded_json(dict(before))
        provider = adapter.invoke(ticket, checked)
        if not isinstance(provider, Mapping):
            raise DomainError("INVALID_PROVIDER_RESPONSE")
        frozen_provider = _bounded_json(dict(provider))
        after = adapter.observe(ticket)
        if not isinstance(after, Mapping):
            raise DomainError("EXECUTION_POST_OBSERVATION_MISSING")
        frozen_after = _bounded_json(dict(after))
        # Both observations are adapter-derived; policy must independently verify.
        # The combined packet must also fit the workflow's bounded result format.
        packet = {"before": frozen_before, "provider": frozen_provider, "after": frozen_after}
        return _bounded_json(packet)

    def stop(self, ticket: ExecutionTicket) -> str:
        adapter = self._adapters.get(ticket.tool_id)
        if adapter is None:
            return "UNKNOWN"
        try:
            ack = adapter.request_stop(ticket)
        except Exception:
            return "UNKNOWN"
        return ack if ack in {"CONFIRMED", "REJECTED", "UNKNOWN"} else "UNKNOWN"
