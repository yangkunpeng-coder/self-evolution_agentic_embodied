"""R09d: immutable, fail-closed rollback requests for trusted R0-only runtime.

No prior *distinct* trusted deployed release exists. All valid requests are
recorded as BLOCKED; neither the active pointer nor pinned tasks are changed.
Physical actions are never reversible by a database transaction.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import cast

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError
from ..core.ids import uid, utc_now
from ..storage.sqlite.database import R0_MANIFEST, SqliteStore
from .release_management import ReleaseManager, ReleasePointer


@dataclass(frozen=True)
class RollbackAttempt:
    attempt_id: str
    request_key: str
    target_release_id: str
    expected: ReleasePointer
    status: str
    reason: str
    rationale: str
    audit_hash: str


class RollbackManager:
    """Record safe, idempotent denial; never claim a successful rollback.

    All writes use the same SQLite transaction and worker fence as the
    execution/release governance subsystem. The only executable release is R0.
    """

    DENIAL = "NO_DISTINCT_TRUSTED_RELEASE_OR_ROLLBACK_AUTHORITY"

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    def active(self) -> ReleasePointer:
        return ReleaseManager(self.store).active()

    @staticmethod
    def _validate_key(value: str) -> bool:
        return (isinstance(value, str) and 1 <= len(value) <= 120
                and value.isascii() and all(ch.isalnum() or ch in "_-" for ch in value))

    @staticmethod
    def _validate_rationale(value: str) -> bool:
        return (isinstance(value, str) and 1 <= len(value.strip()) <= 500
                and value == value.strip() and "\x00" not in value)

    @staticmethod
    def _trusted_target(c: sqlite3.Connection, release_id: str) -> None:
        """A research draft is never an executable release target."""
        if release_id != "R0-FAKE":
            raise DomainError("ROLLBACK_TARGET_NOT_TRUSTED")
        row = c.execute(
            "SELECT body,body_hash FROM release_manifests WHERE release_id=?", (release_id,),
        ).fetchone()
        if (row is None or row["body"] != canonical_bytes(R0_MANIFEST).decode("utf-8")
            or row["body_hash"] != content_hash(R0_MANIFEST)):
            raise DomainError("ROLLBACK_TARGET_NOT_TRUSTED")

    @classmethod
    def _document(
        cls, request_key: str, target_release_id: str, expected: ReleasePointer, rationale: str,
    ) -> dict[str, str | int | bool]:
        return {
            "schema_version": 1,
            "request_key": request_key,
            "target_release_id": target_release_id,
            "expected_release_id": expected.release_id,
            "expected_manifest_hash": expected.manifest_hash,
            "expected_revision": expected.revision,
            "rationale": rationale,
            "status": "BLOCKED",
            "reason": cls.DENIAL,
            "active_release_modified": False,
            "historical_tasks_modified": False,
            "physical_actions_reversed": False,
        }

    @classmethod
    def _record(cls, row: sqlite3.Row) -> RollbackAttempt:
        pointer = ReleasePointer(
            cast(str, row["expected_release_id"]),
            cast(str, row["expected_manifest_hash"]),
            cast(int, row["expected_revision"]),
        )
        data = cls._document(
            cast(str, row["request_key"]), cast(str, row["target_release_id"]),
            pointer, cast(str, row["rationale"]),
        )
        try:
            persisted = json.loads(cast(str, row["attempt_json"]))
            if (not isinstance(persisted, dict)
                or canonical_bytes(persisted) != canonical_bytes(data)
                or row["attempt_json"] != canonical_bytes(data).decode("utf-8")
                or row["attempt_hash"] != content_hash(data)):
                raise DomainError("ROLLBACK_AUDIT_TAMPERED")
        except (ValueError, TypeError, KeyError) as exc:
            raise DomainError("ROLLBACK_AUDIT_TAMPERED") from exc
        return RollbackAttempt(
            cast(str, row["attempt_id"]), cast(str, row["request_key"]),
            cast(str, row["target_release_id"]), pointer, "BLOCKED", cls.DENIAL,
            cast(str, row["rationale"]), content_hash(data),
        )

    def get_attempt(self, request_key: str) -> RollbackAttempt | None:
        if not self._validate_key(request_key):
            raise DomainError("INVALID_ROLLBACK_REQUEST")
        with closing(self.store._connect()) as c:
            row = c.execute(
                "SELECT * FROM release_rollback_attempts WHERE request_key=?", (request_key,),
            ).fetchone()
            return None if row is None else self._record(row)

    def request_rollback(
        self, target_release_id: str, *, expected: ReleasePointer,
        request_key: str, rationale: str,
    ) -> RollbackAttempt:
        """Apply CAS/fence checks, commit immutable BLOCKED audit, NEVER move pointer.

        Research-review acceptance and a research draft are not rollback authority.
        No previous published version exists to which a meaningful rollback can occur.
        """
        if (not isinstance(target_release_id, str) or not target_release_id
            or not isinstance(expected, ReleasePointer)
            or not self._validate_key(request_key)
            or not self._validate_rationale(rationale)):
            raise DomainError("INVALID_ROLLBACK_REQUEST")
        with self.store._write() as c:
            current = ReleaseManager._active(c)
            if current != expected:
                raise DomainError("STALE_RELEASE_POINTER")
            self._trusted_target(c, target_release_id)
            prior = c.execute(
                "SELECT * FROM release_rollback_attempts WHERE request_key=?", (request_key,),
            ).fetchone()
            if prior is not None:
                result = self._record(prior)
                if (result.target_release_id != target_release_id
                    or result.expected != expected or result.rationale != rationale):
                    raise DomainError("ROLLBACK_IDEMPOTENCY_CONFLICT")
                return result
            data = self._document(request_key, target_release_id, expected, rationale)
            c.execute(
                "INSERT INTO release_rollback_attempts "
                "(attempt_id,request_key,target_release_id,expected_release_id,"
                "expected_manifest_hash,expected_revision,rationale,attempt_json,"
                "attempt_hash,attempted_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (uid(), request_key, target_release_id, expected.release_id,
                 expected.manifest_hash, expected.revision, rationale,
                 canonical_bytes(data).decode("utf-8"), content_hash(data), utc_now()),
            )
            row = c.execute(
                "SELECT * FROM release_rollback_attempts WHERE request_key=?", (request_key,),
            ).fetchone()
            if row is None:
                raise DomainError("ROLLBACK_AUDIT_WRITE_FAILED")
            return self._record(row)
