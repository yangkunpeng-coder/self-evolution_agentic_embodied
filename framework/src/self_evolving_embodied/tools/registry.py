"""Process-local trusted tool catalog and read-only routing boundary."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..core.contracts import DomainError
from .contracts import Effect, ToolHandler, ToolSpec, _bounded_json, check_arguments


@dataclass(frozen=True)
class ToolResult:
    tool_id: str
    output: dict[str, Any]
    verified: bool = False  # tool output alone never proves a task postcondition


class PluggableToolRegistry:
    """Separate from SqliteSkillRegistry, which governs published executable skills.

    Only trusted application code can register/bind implementations. Generated
    model output cannot register tools, import plugins, or grant capabilities.
    """

    def __init__(self) -> None:
        self._specs: dict[str, ToolSpec] = {}
        self._handlers: dict[str, ToolHandler] = {}

    def register(self, spec: ToolSpec, handler: ToolHandler | None = None) -> None:
        if not isinstance(spec, ToolSpec):
            raise DomainError("INVALID_TOOL_SPEC")
        if spec.tool_id in self._specs:
            raise DomainError("TOOL_ALREADY_REGISTERED")
        if handler is not None and spec.effect is not Effect.READ_ONLY:
            raise DomainError("EFFECTFUL_TOOL_REQUIRES_HARNESS")
        if handler is not None and not callable(getattr(handler, "invoke", None)):
            raise DomainError("INVALID_TOOL_HANDLER")
        self._specs[spec.tool_id] = spec
        if handler is not None:
            self._handlers[spec.tool_id] = handler

    def bind(self, tool_id: str, handler: ToolHandler) -> None:
        spec = self.resolve(tool_id)
        if spec.effect is not Effect.READ_ONLY:
            raise DomainError("EFFECTFUL_TOOL_REQUIRES_HARNESS")
        if not callable(getattr(handler, "invoke", None)):
            raise DomainError("INVALID_TOOL_HANDLER")
        self._handlers[tool_id] = handler

    def is_bound(self, tool_id: str) -> bool:
        self.resolve(tool_id)
        return tool_id in self._handlers

    def resolve(self, tool_id: str) -> ToolSpec:
        try:
            return self._specs[tool_id]
        except (KeyError, TypeError):
            raise DomainError("UNKNOWN_TOOL") from None

    def find_by_capability(self, capability: str) -> tuple[ToolSpec, ...]:
        """Deterministic discovery; never selects or authorizes a tool implicitly."""
        if not isinstance(capability, str) or not capability.strip():
            raise DomainError("INVALID_CAPABILITY")
        return tuple(self._specs[key] for key in sorted(self._specs)
                     if self._specs[key].capability == capability)

    def catalog(self) -> list[dict[str, Any]]:
        return [self._specs[k].public_description() for k in sorted(self._specs)]

    def dispatch_read_only(self, tool_id: str, arguments: Mapping[str, Any]) -> ToolResult:
        """Run informational models only. PHYSICAL_ACTION requires durable Harness ticket.

        This method is *not* a replacement for event-sourced effectful execution.
        Outputs are explicitly unverified and must not be treated as evidence.
        """
        spec = self.resolve(tool_id)
        if spec.effect != Effect.READ_ONLY:
            raise DomainError("EFFECTFUL_TOOL_REQUIRES_HARNESS")
        checked = check_arguments(spec, dict(arguments))
        handler = self._handlers.get(tool_id)
        if handler is None:
            raise DomainError("TOOL_NOT_BOUND")
        try:
            result = handler.invoke(checked)
        except Exception:
            raise DomainError("TOOL_PROVIDER_FAILED") from None
        if not isinstance(result, Mapping) or not all(type(k) is str for k in result):
            raise DomainError("INVALID_TOOL_RESULT")
        frozen = _bounded_json(dict(result))
        return ToolResult(tool_id=tool_id, output=frozen)
