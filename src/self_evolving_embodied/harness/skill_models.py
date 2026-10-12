"""Declarative, non-executable, immutable Skill contract (R05)."""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from enum import StrEnum

from ..core.canonical_json import content_hash
from ..core.contracts import DomainError


class SkillLifecycle(StrEnum):
    DRAFT = "DRAFT"
    VALIDATED = "VALIDATED"
    PUBLISHED = "PUBLISHED"
    DEPRECATED = "DEPRECATED"


_ID = re.compile(r"^[a-zA-Z][a-zA-Z0-9._-]{0,63}$")
_ALLOWED_FIELD_TYPES = {"str", "int", "bool"}


@dataclass(frozen=True)
class SkillDefinition:
    skill_id: str
    version: int
    release_id: str
    tool_id: str
    capability_id: str
    resource: str
    description: str
    argument_schema: dict[str, str]
    result_schema: dict[str, str]

    def __post_init__(self) -> None:
        for name in (self.skill_id, self.release_id, self.tool_id,
                     self.capability_id, self.resource):
            if not isinstance(name, str) or not _ID.fullmatch(name):
                raise DomainError("INVALID_SKILL_IDENTIFIER")
        if type(self.version) is not int or not 1 <= self.version <= 1_000_000:
            raise DomainError("INVALID_SKILL_VERSION")
        if (not isinstance(self.description, str) or not self.description.strip()
            or len(self.description) > 500):
            raise DomainError("INVALID_SKILL_DESCRIPTION")
        for schema in (self.argument_schema, self.result_schema):
            if (not isinstance(schema, dict) or not schema or len(schema) > 32
                or not all(isinstance(key, str) and key.isidentifier()
                           and not key.startswith("_") and len(key) <= 64
                           and type(kind) is str and kind in _ALLOWED_FIELD_TYPES
                           for key, kind in schema.items())):
                raise DomainError("INVALID_SKILL_SCHEMA")

    @property
    def digest(self) -> str:
        return content_hash(asdict(self))


R0_SKILL = SkillDefinition(
    skill_id="move_to_ready", version=1, release_id="R0-FAKE",
    tool_id="fake.move", capability_id="fake.move", resource="fake-arm",
    description="Built-in deterministic Fake move (no executable skill code)",
    argument_schema={"target": "str"},
    result_schema={"position": "str", "goal_reached": "bool"},
)
