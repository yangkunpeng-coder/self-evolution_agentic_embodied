"""R05 versioned declarative Skill Registry; never loads untrusted code.

Only a pinned built-in R0 skill is PUBLISHED. New skill versions may be
registered/validated by the trusted in-process administration port but cannot
be published without the future release approval protocol.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from typing import cast

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import (
    AuthorizedInvocation,
    DomainError,
    PlanStep,
    TaskContract,
    ToolDescriptor,
)
from ..core.ids import uid, utc_now
from ..storage.sqlite.database import SqliteStore
from .skill_models import R0_SKILL, SkillDefinition, SkillLifecycle


@dataclass(frozen=True)
class SkillDescription:
    """Compatibility with the read-only Step 6 information-only registry."""

    skill_id: str
    release_id: str
    purpose: str


class ReadOnlySkillRegistry:
    """Legacy informational view; NOT used for runtime authorization."""

    def list_skills(self) -> tuple[SkillDescription, ...]:
        return (SkillDescription("move_to_ready", "R0-FAKE", "Fake move primitive"),)


class SqliteSkillRegistry:
    """Trusted local process registry; store fencing applies to writes.

    Reviewer strings/evidence references are audit metadata, NOT proof of
    identity: no remote administrative endpoint is available in R05.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _audit(c: sqlite3.Connection, skill_id: str, version: int,
               event: str, details: dict[str, str]) -> None:
        c.execute(
            "INSERT INTO skill_audit (event_id,skill_id,version,event_type,details,recorded_at) "
            "VALUES (?,?,?,?,?,?)",
            (uid(), skill_id, version, event,
             canonical_bytes(details).decode("utf-8"), utc_now()),
        )

    @staticmethod
    def _lookup(c: sqlite3.Connection, skill_id: str, version: int) -> sqlite3.Row:
        row = c.execute(
            "SELECT d.definition_json,d.definition_hash,s.state FROM skill_definitions AS d "
            "JOIN skill_states AS s ON s.skill_id=d.skill_id AND s.version=d.version "
            "WHERE d.skill_id=? AND d.version=?", (skill_id, version),
        ).fetchone()
        if row is None:
            raise DomainError("SKILL_NOT_FOUND")
        return cast(sqlite3.Row, row)

    def register_draft(self, definition: SkillDefinition) -> str:
        if not isinstance(definition, SkillDefinition):
            raise DomainError("INVALID_SKILL_DEFINITION")
        if definition == R0_SKILL:
            raise DomainError("BUILTIN_SKILL_IMMUTABLE")
        with self.store._write() as c:
            if c.execute("SELECT 1 FROM release_manifests WHERE release_id=?",
                         (definition.release_id,)).fetchone() is None:
                raise DomainError("UNKNOWN_RELEASE")
            last = c.execute("SELECT MAX(version) FROM skill_definitions WHERE skill_id=?",
                             (definition.skill_id,)).fetchone()[0]
            expected = 1 if last is None else cast(int, last) + 1
            if definition.version != expected:
                raise DomainError("SKILL_VERSION_CONFLICT")
            body = canonical_bytes(asdict(definition)).decode("utf-8")
            c.execute("INSERT INTO skill_definitions VALUES (?,?,?,?,?,?,?)",
                      (definition.skill_id, definition.version, definition.release_id,
                       definition.tool_id, body, definition.digest, utc_now()))
            c.execute("INSERT INTO skill_states VALUES (?,?,?)",
                      (definition.skill_id, definition.version, SkillLifecycle.DRAFT.value))
            self._audit(c, definition.skill_id, definition.version, "SKILL_REGISTERED",
                        {"digest": definition.digest})
        return definition.digest

    def validate(self, skill_id: str, version: int, *, reviewer: str,
                 evidence_ref: str) -> None:
        if (not isinstance(reviewer, str) or not 1 <= len(reviewer.strip()) <= 128
            or not isinstance(evidence_ref, str) or not 1 <= len(evidence_ref.strip()) <= 256):
            raise DomainError("SKILL_REVIEW_REQUIRED")
        with self.store._write() as c:
            row = self._lookup(c, skill_id, version)
            if row["state"] != SkillLifecycle.DRAFT.value:
                raise DomainError("SKILL_INVALID_TRANSITION")
            self._decode(row)  # detect tampered/corrupt immutable contents
            c.execute("UPDATE skill_states SET state='VALIDATED' "
                      "WHERE skill_id=? AND version=? AND state='DRAFT'", (skill_id, version))
            self._audit(c, skill_id, version, "SKILL_VALIDATED",
                        {"reviewer": reviewer.strip(), "evidence_ref": evidence_ref.strip()})

    def publish(self, skill_id: str, version: int) -> None:
        """Fail closed until a release manifest + human approval are implemented."""
        raise DomainError("SKILL_PUBLICATION_DISABLED")

    def deprecate(self, skill_id: str, version: int, *, reason: str) -> None:
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 256:
            raise DomainError("SKILL_DEPRECATION_REASON_REQUIRED")
        with self.store._write() as c:
            row = self._lookup(c, skill_id, version)
            if row["state"] not in {SkillLifecycle.DRAFT.value, SkillLifecycle.VALIDATED.value}:
                raise DomainError("SKILL_PINNED_OR_TERMINAL")
            c.execute("UPDATE skill_states SET state='DEPRECATED' "
                      "WHERE skill_id=? AND version=?", (skill_id, version))
            self._audit(c, skill_id, version, "SKILL_DEPRECATED", {"reason": reason.strip()})

    @staticmethod
    def _decode(row: sqlite3.Row) -> SkillDefinition:
        try:
            body = json.loads(cast(str, row["definition_json"]))
            if not isinstance(body, dict):
                raise TypeError("not an object")
            spec = SkillDefinition(**body)
        except (ValueError, TypeError, KeyError) as exc:
            raise DomainError("SKILL_DEFINITION_CORRUPT") from exc
        if spec.digest != row["definition_hash"]:
            raise DomainError("SKILL_DEFINITION_CORRUPT")
        return spec

    def list_skills(self, *, release_id: str = "R0-FAKE") -> list[dict[str, object]]:
        with closing(self.store._connect()) as c:
            rows = c.execute(
                "SELECT d.definition_json,d.definition_hash,s.state "
                "FROM skill_definitions d JOIN skill_states s "
                "ON d.skill_id=s.skill_id AND d.version=s.version "
                "WHERE d.release_id=? ORDER BY d.skill_id,d.version", (release_id,),
            ).fetchall()
            result: list[dict[str, object]] = []
            for row in rows:
                spec = self._decode(row)
                result.append({"skill_id": spec.skill_id, "version": spec.version,
                               "digest": cast(str, row["definition_hash"]),
                               "lifecycle": cast(str, row["state"])})
            return result

    def resolve(self, task: TaskContract, tool_id: str) -> ToolDescriptor:
        """Only a manifest-pinned, immutable and published skill may be executed."""
        with closing(self.store._connect()) as c:
            pinned = c.execute(
                "SELECT t.release_id,t.release_hash,m.body_hash "
                "FROM tasks t JOIN release_manifests m ON t.release_id=m.release_id "
                "WHERE t.task_id=?", (task.task_id,),
            ).fetchone()
            if (pinned is None or pinned["release_id"] != task.release_id
                or pinned["release_hash"] != pinned["body_hash"]):
                raise DomainError("SKILL_RELEASE_NOT_PINNED")
            row = c.execute(
                "SELECT b.definition_hash AS binding_hash,d.definition_json,d.definition_hash,"
                "s.state FROM release_skill_bindings b "
                "JOIN skill_definitions d ON d.skill_id=b.skill_id AND d.version=b.version "
                "JOIN skill_states s ON s.skill_id=b.skill_id AND s.version=b.version "
                "WHERE b.release_id=? AND b.tool_id=?", (task.release_id, tool_id),
            ).fetchone()
            if row is None:
                # Preserve Step 6 error semantics for entirely unknown tools.
                candidate = c.execute(
                    "SELECT 1 FROM skill_definitions WHERE tool_id=? AND release_id=? LIMIT 1",
                    (tool_id, task.release_id),
                ).fetchone()
                raise DomainError("SKILL_NOT_PUBLISHED" if candidate else "UNKNOWN_TOOL")
            spec = self._decode(row)
            if (row["state"] != SkillLifecycle.PUBLISHED.value
                or row["binding_hash"] != spec.digest
                or spec.release_id != task.release_id or spec.tool_id != tool_id):
                raise DomainError("SKILL_BINDING_INVALID")
            if spec.capability_id not in task.allowed_capabilities:
                raise DomainError("CAPABILITY_DENIED")
            return ToolDescriptor(spec.tool_id, spec.capability_id,
                                  tuple(spec.argument_schema), spec.release_id,
                                  resource=spec.resource)

    def record_authorized_intent(
        self, c: sqlite3.Connection, ticket: AuthorizedInvocation, plan: PlanStep,
    ) -> None:
        """Audit the immutable R0 grant inside the intent transaction."""
        bound = c.execute(
            "SELECT b.skill_id,b.version,b.definition_hash,d.definition_json,"
            "d.definition_hash AS actual_hash,s.state FROM release_skill_bindings b "
            "JOIN skill_definitions d ON b.skill_id=d.skill_id AND b.version=d.version "
            "JOIN skill_states s ON b.skill_id=s.skill_id AND b.version=s.version "
            "WHERE b.release_id=? AND b.tool_id=?", (ticket.release_id, ticket.tool_id),
        ).fetchone()
        if bound is None or bound["state"] != SkillLifecycle.PUBLISHED.value:
            raise DomainError("SKILL_NOT_PUBLISHED")
        spec = self._decode(bound)
        if (bound["definition_hash"] != bound["actual_hash"]
            or spec.tool_id != ticket.tool_id or spec.release_id != ticket.release_id
            or spec.resource != ticket.resource or spec.capability_id != plan.capability_id
            or plan.tool_id != ticket.tool_id or plan.release_id != ticket.release_id
            or content_hash(plan.arguments) != ticket.arguments_hash):
            raise DomainError("SKILL_BINDING_INVALID")
        if set(plan.arguments) != set(spec.argument_schema):
            raise DomainError("SKILL_ARGUMENT_SCHEMA_MISMATCH")
        for name, kind in spec.argument_schema.items():
            val = plan.arguments[name]
            allowed = {"str": str, "int": int, "bool": bool}[kind]
            if type(val) is not allowed:
                raise DomainError("SKILL_ARGUMENT_SCHEMA_MISMATCH")
        self._audit(c, spec.skill_id, spec.version, "SKILL_INVOCATION_AUTHORIZED",
                    {"task_id": ticket.task_id, "invocation_id": ticket.invocation_id,
                     "release_id": ticket.release_id, "definition_hash": spec.digest})
