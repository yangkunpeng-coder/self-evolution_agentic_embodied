"""R04 trusted memory, backed by the same SQLite transaction coordinator as CommitGate.

Security boundary: this is an in-process trusted API, not a remotely exposed store.
RAW input is untrusted text. VERIFIED is a projection of existing GOAL commits,
never an editable tag or an assertion supplied by the memory author.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, cast

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError, Status, Verdict
from ..core.ids import uid, utc_now
from ..harness.verifier import IndependentVerifier
from ..storage.sqlite.database import SqliteStore


class MemoryTrust(StrEnum):
    RAW = "RAW"
    VERIFIED = "VERIFIED"


class MemoryKind(StrEnum):
    EPISODIC = "EPISODIC"
    OPERATIONAL = "OPERATIONAL"
    SEMANTIC = "SEMANTIC"
    PROCEDURAL = "PROCEDURAL"


@dataclass(frozen=True)
class MemoryRecord:
    memory_id: str
    kind: MemoryKind
    trust_level: MemoryTrust
    status: str
    content: dict[str, Any]
    content_hash: str
    source_task_id: str
    source_commit_id: str | None
    source_evidence_id: str | None
    source_verdict_id: str | None
    release_id: str
    scope_task_id: str
    expires_at_ms: int | None
    created_at: str
    provenance: dict[str, str]


class MemoryStore(Protocol):
    """Replaceable M15 port. Implementation stays in trusted process."""

    def append_raw(
        self, task_id: str, summary: str, *,
        kind: MemoryKind = MemoryKind.EPISODIC,
        expires_at_ms: int | None = None,
    ) -> str: ...

    def project_pending(self, max_messages: int = 100) -> int: ...

    def search(
        self, *, task_id: str, release_id: str, include_raw: bool = False,
        kind: MemoryKind | None = None, limit: int = 50,
    ) -> list[MemoryRecord]: ...

    def revoke(self, memory_id: str, reason: str) -> bool: ...


class SqliteMemory:
    """M15 local projection and search. Does not grant tool-use or policy authority.

    Mutations participate in R03 fencing via SqliteStore._write; callers must
    protect DB file access and must not expose this object to untrusted agents.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _audit(c: sqlite3.Connection, memory_id: str, event: str, details: dict[str, str]) -> None:
        c.execute(
            "INSERT INTO memory_audit VALUES (?,?,?,?,?)",
            (uid(), memory_id, event, canonical_bytes(details).decode("utf-8"), utc_now()),
        )

    def append_raw(
        self, task_id: str, summary: str, *,
        kind: MemoryKind = MemoryKind.EPISODIC,
        expires_at_ms: int | None = None,
    ) -> str:
        """Store a non-executable raw episode. Never converts raw into Verified.

        The text is *data*, must be minimized and must never be injected as a
        policy/system instruction. Avoid secrets, PII, or hidden chain-of-thought.
        """
        if not isinstance(summary, str) or not summary.strip() or len(summary) > 2048:
            raise DomainError("INVALID_RAW_SUMMARY")
        if not isinstance(kind, MemoryKind):
            raise DomainError("INVALID_MEMORY_KIND")
        now_ms = self.store._now_ms()
        if (expires_at_ms is not None
            and (type(expires_at_ms) is not int or expires_at_ms <= now_ms)):
            raise DomainError("INVALID_MEMORY_EXPIRY")
        memory_id = uid()
        content = {"summary": summary}
        with self.store._write() as c:
            task = self.store._task(c, task_id)
            if task["status"] == Status.CREATED.value:
                raise DomainError("TASK_NOT_STARTED")
            c.execute(
                "INSERT INTO memory_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (memory_id, kind.value, MemoryTrust.RAW.value,
                 canonical_bytes(content).decode("utf-8"), content_hash(content), task_id,
                 None, None, None, task["release_id"], task_id,
                 expires_at_ms, utc_now(),
                 canonical_bytes({"source_type": "UNTRUSTED_EPISODE",
                                  "author": "UNTRUSTED_INPUT"}).decode("utf-8")),
            )
            self._audit(c, memory_id, "MEMORY_RAW_APPENDED", {"task_id": task_id})
        return memory_id

    @staticmethod
    def _validated_goal_source(
        c: sqlite3.Connection, commit_id: str, task_id: str, release_id: str,
    ) -> tuple[dict[str, str], str, str]:
        """Check immutable-projection prerequisites in one DB transaction.

        DB file administrator tampering is outside current SQLite threat model;
        nevertheless corrupted cross-references or payload hashes fail closed.
        """
        row = c.execute(
            "SELECT r.*, t.status, t.goal_json, t.release_id FROM commit_records AS r "
            "JOIN tasks AS t ON t.task_id=r.task_id WHERE r.commit_id=? AND r.task_id=?",
            (commit_id, task_id),
        ).fetchone()
        if (row is None or row["scope"] != "GOAL" or row["status"] != Status.SUCCEEDED.value
            or row["release_id"] != release_id):
            raise DomainError("MEMORY_SOURCE_NOT_COMMITTED")
        evidence = c.execute(
            "SELECT * FROM evidences WHERE evidence_id=? AND task_id=?",
            (row["evidence_id"], task_id),
        ).fetchone()
        verdict = c.execute(
            "SELECT * FROM verdicts WHERE verdict_id=? AND evidence_id=? AND task_id=?",
            (row["verdict_id"], row["evidence_id"], task_id),
        ).fetchone()
        if evidence is None or verdict is None:
            raise DomainError("MEMORY_EVIDENCE_MISSING")
        try:
            e = json.loads(cast(str, evidence["payload"]))
            v = json.loads(cast(str, verdict["payload"]))
            goal = json.loads(cast(str, row["goal_json"]))
            if not all(isinstance(item, dict) for item in (e, v, goal)):
                raise ValueError("not an object")
            valid = (
                evidence["evidence_hash"] == content_hash(e)
                and verdict["verdict_hash"] == content_hash(v)
                and v["evidence_hash"] == evidence["evidence_hash"]
                and v["result"] == Verdict.SUCCESS.value
                and v["verifier_id"] == IndependentVerifier.ID
                and v["condition_id"] == goal["condition_id"]
                and e["scope"] == v["scope"] == "GOAL"
                and e["task_id"] == v["task_id"] == task_id
                and e["release_id"] == v["release_id"] == release_id
                and e["evidence_id"] == row["evidence_id"]
                and v["verdict_id"] == row["verdict_id"]
                and e["step_id"] is None and v["step_id"] is None
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise DomainError("MEMORY_PROVENANCE_INVALID") from exc
        if not valid:
            raise DomainError("MEMORY_PROVENANCE_INVALID")
        content = {"type": "GOAL_VERIFIED", "task_id": task_id,
                   "release_id": release_id, "condition_id": cast(str, goal["condition_id"]),
                   "evidence_hash": cast(str, evidence["evidence_hash"])}
        return content, cast(str, row["evidence_id"]), cast(str, row["verdict_id"])

    def project_pending(self, max_messages: int = 100) -> int:
        """Idempotently materialize Verified episodes and consume matching outbox.

        GOAL commit was durable before projection. On error everything in this
        projection transaction is rolled back and the message remains pending.
        """
        if not 1 <= max_messages <= 1000:
            raise DomainError("INVALID_BATCH_LIMIT")
        count = 0
        with self.store._write() as c:
            rows = c.execute(
                "SELECT message_id,task_id,payload FROM outbox "
                "WHERE topic='MEMORY_VERIFIED_PENDING' AND consumed=0 "
                "ORDER BY rowid LIMIT ?", (max_messages,),
            ).fetchall()
            for row in rows:
                try:
                    payload = json.loads(cast(str, row["payload"]))
                    if (not isinstance(payload, dict)
                        or set(payload) != {"commit_id", "task_id", "release_id"}
                        or payload["task_id"] != row["task_id"]):
                        raise ValueError("invalid outbox envelope")
                    commit_id = cast(str, payload["commit_id"])
                    task_id = cast(str, payload["task_id"])
                    release_id = cast(str, payload["release_id"])
                    content, evidence_id, verdict_id = self._validated_goal_source(
                        c, commit_id, task_id, release_id,
                    )
                except (ValueError, TypeError, KeyError) as exc:
                    raise DomainError("INVALID_MEMORY_OUTBOX") from exc
                existing = c.execute(
                    "SELECT * FROM memory_records WHERE source_commit_id=?", (commit_id,),
                ).fetchone()
                if existing is None:
                    memory_id = uid()
                    c.execute(
                        "INSERT INTO memory_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (memory_id, MemoryKind.EPISODIC.value, MemoryTrust.VERIFIED.value,
                         canonical_bytes(content).decode("utf-8"), content_hash(content),
                         task_id, commit_id, evidence_id, verdict_id, release_id, task_id,
                         None, utc_now(),
                         canonical_bytes({"source_type": "TRUSTED_COMMIT_PROJECTION",
                                          "author": "MEMORY_PROJECTOR"}).decode("utf-8")),
                    )
                    self._audit(c, memory_id, "MEMORY_VERIFIED", {"commit_id": commit_id})
                else:
                    self._check_projected_row(existing, content, task_id, release_id,
                                              evidence_id, verdict_id)
                c.execute("UPDATE outbox SET consumed=1 WHERE message_id=?", (row["message_id"],))
                count += 1
        return count

    @staticmethod
    def _check_projected_row(
        row: sqlite3.Row, content: dict[str, str], task_id: str,
        release_id: str, evidence_id: str, verdict_id: str,
    ) -> None:
        if (row["trust_level"] != MemoryTrust.VERIFIED.value
            or row["kind"] != MemoryKind.EPISODIC.value
            or row["content_hash"] != content_hash(content)
            or row["content_json"] != canonical_bytes(content).decode("utf-8")
            or row["source_task_id"] != task_id or row["scope_task_id"] != task_id
            or row["release_id"] != release_id
            or row["source_evidence_id"] != evidence_id
            or row["source_verdict_id"] != verdict_id):
            raise DomainError("MEMORY_PROJECTION_CONFLICT")

    def search(
        self, *, task_id: str, release_id: str, include_raw: bool = False,
        kind: MemoryKind | None = None, limit: int = 50,
    ) -> list[MemoryRecord]:
        """Fail-closed exact-scope lookup; RAW is never returned by default.

        No cross-task retrieval or global semantic policy promotion in R04.
        """
        if (not task_id or not release_id or not 1 <= limit <= 100
            or type(include_raw) is not bool):
            raise DomainError("INVALID_MEMORY_QUERY")
        if kind is not None and not isinstance(kind, MemoryKind):
            raise DomainError("INVALID_MEMORY_KIND")
        results: list[MemoryRecord] = []
        with closing(self.store._connect()) as c:
            query = (
                "SELECT m.* FROM memory_records m "
                "LEFT JOIN memory_revocations r ON r.memory_id=m.memory_id "
                "WHERE m.scope_task_id=? AND m.release_id=? AND r.memory_id IS NULL "
                "AND (m.expires_at_ms IS NULL OR m.expires_at_ms>?) "
            )
            params: list[str | int] = [task_id, release_id, self.store._now_ms()]
            if not include_raw:
                query += "AND m.trust_level='VERIFIED' "
            if kind is not None:
                query += "AND m.kind=? "
                params.append(kind.value)
            query += "ORDER BY m.created_at DESC, m.memory_id LIMIT ?"
            params.append(limit)
            for row in c.execute(query, params):
                try:
                    content = json.loads(cast(str, row["content_json"]))
                    provenance = json.loads(cast(str, row["provenance"]))
                    if (not isinstance(content, dict) or not isinstance(provenance, dict)
                        or content_hash(content) != row["content_hash"]):
                        raise DomainError("MEMORY_CORRUPT")
                except (ValueError, TypeError) as exc:
                    raise DomainError("MEMORY_CORRUPT") from exc
                if row["trust_level"] == MemoryTrust.VERIFIED.value:
                    if row["source_commit_id"] is None:
                        raise DomainError("MEMORY_PROVENANCE_INVALID")
                    trusted, e_id, v_id = self._validated_goal_source(
                        c, row["source_commit_id"], row["source_task_id"], row["release_id"],
                    )
                    self._check_projected_row(row, trusted, row["source_task_id"],
                                              row["release_id"], e_id, v_id)
                results.append(MemoryRecord(
                    memory_id=cast(str, row["memory_id"]),
                    kind=MemoryKind(row["kind"]), trust_level=MemoryTrust(row["trust_level"]),
                    status="ACTIVE", content=content, content_hash=cast(str, row["content_hash"]),
                    source_task_id=cast(str, row["source_task_id"]),
                    source_commit_id=cast(str | None, row["source_commit_id"]),
                    source_evidence_id=cast(str | None, row["source_evidence_id"]),
                    source_verdict_id=cast(str | None, row["source_verdict_id"]),
                    release_id=cast(str, row["release_id"]),
                    scope_task_id=cast(str, row["scope_task_id"]),
                    expires_at_ms=cast(int | None, row["expires_at_ms"]),
                    created_at=cast(str, row["created_at"]),
                    provenance=provenance,
                ))
        return results

    def revoke(self, memory_id: str, reason: str) -> bool:
        """Append-only local operator revocation. No IAM/remote endpoint in R04."""
        if not memory_id or not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
            raise DomainError("INVALID_REVOCATION")
        with self.store._write() as c:
            record = c.execute(
                "SELECT 1 FROM memory_records WHERE memory_id=?", (memory_id,),
            ).fetchone()
            if record is None:
                raise DomainError("MEMORY_NOT_FOUND")
            previous = c.execute(
                "SELECT reason FROM memory_revocations WHERE memory_id=?", (memory_id,),
            ).fetchone()
            if previous is not None:
                if previous["reason"] == reason:
                    return False
                raise DomainError("REVOCATION_CONFLICT")
            c.execute("INSERT INTO memory_revocations VALUES (?,?,?)",
                      (memory_id, reason, utc_now()))
            self._audit(c, memory_id, "MEMORY_REVOKED", {"reason": reason})
            return True

    def pending_count(self) -> int:
        with closing(self.store._connect()) as c:
            return cast(int, c.execute(
                "SELECT COUNT(*) FROM outbox "
                "WHERE topic='MEMORY_VERIFIED_PENDING' AND consumed=0"
            ).fetchone()[0])
