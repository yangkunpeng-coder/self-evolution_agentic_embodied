"""Model-agnostic tool definitions. Descriptions are metadata, not authority."""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping, Protocol

from ..core.contracts import DomainError

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_TYPES = frozenset({"string", "integer", "number", "boolean", "object", "array"})


class ToolKind(StrEnum):
    VLM = "VLM"
    VLN = "VLN"
    VLA = "VLA"
    WAM = "WAM"
    CLASSICAL_PLANNER = "CLASSICAL_PLANNER"
    CONTROLLER = "CONTROLLER"
    CUSTOM = "CUSTOM"


class Effect(StrEnum):
    READ_ONLY = "READ_ONLY"
    PHYSICAL_ACTION = "PHYSICAL_ACTION"


@dataclass(frozen=True)
class ToolSpec:
    """Trusted, declarative capability descriptor; not a plugin import path."""

    tool_id: str
    capability: str
    kind: ToolKind
    description: str
    argument_types: tuple[tuple[str, str], ...]
    success_conditions: tuple[str, ...]
    version: str = "1.0.0"
    effect: Effect = Effect.PHYSICAL_ACTION

    def __post_init__(self) -> None:
        if (not isinstance(self.tool_id, str) or not _IDENTIFIER.fullmatch(self.tool_id)
            or not isinstance(self.capability, str) or not _IDENTIFIER.fullmatch(self.capability)):
            raise DomainError("INVALID_TOOL_ID")
        if not isinstance(self.kind, ToolKind) or not isinstance(self.effect, Effect):
            raise DomainError("INVALID_TOOL_SPEC")
        if not isinstance(self.description, str) or not 1 <= len(self.description.strip()) <= 300:
            raise DomainError("INVALID_TOOL_DESCRIPTION")
        if not isinstance(self.version, str) or not 1 <= len(self.version) <= 40:
            raise DomainError("INVALID_TOOL_VERSION")
        if (not isinstance(self.argument_types, tuple) or not self.argument_types
            or len(self.argument_types) > 20):
            raise DomainError("INVALID_TOOL_ARGUMENT_SCHEMA")
        fields = []
        for item in self.argument_types:
            if (not isinstance(item, tuple) or len(item) != 2
                or not isinstance(item[0], str) or not item[0].isidentifier()
                or item[0].startswith("_") or len(item[0]) > 64
                or not isinstance(item[1], str) or item[1] not in _TYPES):
                raise DomainError("INVALID_TOOL_ARGUMENT_SCHEMA")
            fields.append(item[0])
        if len(set(fields)) != len(fields):
            raise DomainError("DUPLICATE_TOOL_ARGUMENT")
        if (not isinstance(self.success_conditions, tuple) or not self.success_conditions
            or len(self.success_conditions) > 20
            or any(not isinstance(x, str) or not _IDENTIFIER.fullmatch(x)
                   for x in self.success_conditions)
            or len(set(self.success_conditions)) != len(self.success_conditions)):
            raise DomainError("INVALID_TOOL_SUCCESS_CONDITION")

    def public_description(self) -> dict[str, Any]:
        """The only capability metadata allowed in the LLM prompt."""
        return {
            "tool_id": self.tool_id,
            "capability": self.capability,
            "kind": self.kind.value,
            "version": self.version,
            "description": self.description,
            "argument_types": dict(self.argument_types),
            "success_conditions": list(self.success_conditions),
            "effect": self.effect.value,
        }


class ToolHandler(Protocol):
    """A trusted Python implementation. Never instantiate from model output."""

    def invoke(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]: ...


def check_arguments(spec: ToolSpec, arguments: object) -> dict[str, Any]:
    """Strict top-level schema with bounded JSON; no implicit coercion."""
    if not isinstance(arguments, dict) or any(type(k) is not str for k in arguments):
        raise DomainError("INVALID_TOOL_ARGUMENTS")
    expected = dict(spec.argument_types)
    if set(arguments) != set(expected):
        raise DomainError("TOOL_ARGUMENT_SCHEMA_MISMATCH")
    for key, kind in expected.items():
        value = arguments[key]
        correct = {
            "string": type(value) is str,
            "integer": type(value) is int,
            "number": type(value) in (int, float),
            "boolean": type(value) is bool,
            "object": type(value) is dict,
            "array": type(value) is list,
        }[kind]
        if not correct:
            raise DomainError("TOOL_ARGUMENT_TYPE_MISMATCH")
    return _bounded_json(arguments)


def _bounded_json(value: object) -> Any:
    """JSON round-trip freezes caller-owned data, rejects NaN and oversize inputs."""
    import json

    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True)
        if len(encoded.encode("utf-8")) > 8192:
            raise DomainError("TOOL_PAYLOAD_TOO_LARGE")
        return json.loads(encoded)
    except (TypeError, ValueError, RecursionError, OverflowError):
        raise DomainError("INVALID_TOOL_JSON") from None
