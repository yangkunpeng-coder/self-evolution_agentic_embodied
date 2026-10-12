"""R09c: immutable *non-deployable* candidate release drafts and audited denial.

No executable candidate delta, held-out embodied efficacy, publishing credential,
or rollback interface exists in R09a/R09b. Therefore this module deliberately
contains NO operation capable of changing the active release.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any, cast

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError
from ..core.ids import uid, utc_now
from ..evaluation.evaluator import CandidateEvaluator
from ..review.service import ReviewService
from ..storage.sqlite.database import R0_MANIFEST, SqliteStore


@dataclass(frozen=True)
class ReleasePointer:
    release_id: str
    manifest_hash: str
    revision: int


@dataclass(frozen=True)
class StagedRelease:
    staged_release_id: str
    candidate_id: str
    manifest: dict[str, Any]
    manifest_hash: str


@dataclass(frozen=True)
class ActivationAttempt:
    attempt_id: str
    request_key: str
    staged_release_id: str
    status: str
    reason: str
    content_hash: str


class ReleaseManager:
    """Single local SQLite manager; all writes respect the worker fence.

    A research review is not authorization to deploy. The only trusted runtime
    release remains the compiled-in R0-FAKE until an independent, future
    publishing authorization mechanism has been implemented and reviewed.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _active(c: sqlite3.Connection) -> ReleasePointer:
        pointer = c.execute(
            "SELECT release_id,manifest_hash,revision FROM release_pointer_state "
            "WHERE singleton=1"
        ).fetchone()
        active = c.execute(
            "SELECT release_id FROM active_release WHERE singleton=1"
        ).fetchone()
        manifest = c.execute(
            "SELECT body,body_hash FROM release_manifests WHERE release_id='R0-FAKE'"
        ).fetchone()
        expected = content_hash(R0_MANIFEST)
        if (pointer is None or active is None or manifest is None
            or pointer["release_id"] != "R0-FAKE"
            or active["release_id"] != pointer["release_id"]
            or pointer["manifest_hash"] != expected or pointer["revision"] != 0
            or manifest["body_hash"] != expected
            or manifest["body"] != canonical_bytes(R0_MANIFEST).decode("utf-8")):
            raise DomainError("UNTRUSTED_RELEASE_POINTER")
        return ReleasePointer("R0-FAKE", expected, 0)

    def active(self) -> ReleasePointer:
        with closing(self.store._connect()) as c:
            return self._active(c)

    @staticmethod
    def _provenance(c: sqlite3.Connection, candidate_id: str) -> dict[str, Any]:
        request_row = c.execute(
            "SELECT * FROM candidate_review_requests WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        if request_row is None:
            raise DomainError("RELEASE_RESEARCH_REVIEW_REQUIRED")
        request = ReviewService._request_record(c, request_row)
        decision_row = c.execute(
            "SELECT * FROM candidate_review_decisions WHERE request_id=?",
            (request.request_id,),
        ).fetchone()
        if decision_row is None:
            raise DomainError("RELEASE_RESEARCH_REVIEW_REQUIRED")
        decision = ReviewService._decision_record(c, decision_row)
        if decision.choice != "ACCEPT_FOR_RESEARCH":
            raise DomainError("RELEASE_RESEARCH_REVIEW_REJECTED")
        evaluation_row = c.execute(
            "SELECT * FROM candidate_evaluations WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        if evaluation_row is None:
            raise DomainError("RELEASE_EVALUATION_REQUIRED")
        evaluation = CandidateEvaluator._record(c, evaluation_row)
        if (evaluation.report["gate_status"] != "BLOCKED"
            or evaluation.report["promotion_eligible"] is not False
            or decision.content["release_approved"] is not False):
            raise DomainError("RELEASE_INVALID_PROVENANCE")
        return {
            "baseline_release_id": evaluation.baseline_release_id,
            "baseline_release_hash": evaluation.report["baseline_release_hash"],
            "candidate_id": candidate_id,
            "candidate_hash": evaluation.report["candidate_hash"],
            "evaluation_id": evaluation.evaluation_id,
            "evaluation_hash": evaluation.report_hash,
            "review_request_id": request.request_id,
            "review_request_hash": request.content_hash,
            "review_decision_id": decision.decision_id,
            "review_decision_hash": decision.content_hash,
        }

    @classmethod
    def _draft_contents(cls, c: sqlite3.Connection, candidate_id: str) -> dict[str, Any]:
        source = cls._provenance(c, candidate_id)
        return {
            "schema_version": 1,
            "kind": "OFFLINE_RESEARCH_DRAFT",
            "staged_release_id": "R-DRAFT-" + content_hash(source)[:20],
            "source": source,
            "capabilities": [],
            "contains_executable_delta": False,
            "candidate_effect_measured": False,
            "approval_scope": "RESEARCH_ONLY",
            "activation_eligible": False,
            "runtime_resolvable": False,
            "physical_actions_authorized": False,
        }

    @classmethod
    def _draft_record(cls, c: sqlite3.Connection, row: sqlite3.Row) -> StagedRelease:
        candidate_id = cast(str, row["candidate_id"])
        expected = cls._draft_contents(c, candidate_id)
        try:
            content = json.loads(cast(str, row["manifest_json"]))
            if (not isinstance(content, dict)
                or row["staged_release_id"] != expected["staged_release_id"]
                or row["baseline_release_id"] != expected["source"]["baseline_release_id"]
                or row["baseline_release_hash"] != expected["source"]["baseline_release_hash"]
                or canonical_bytes(content) != canonical_bytes(expected)
                or row["manifest_json"] != canonical_bytes(expected).decode("utf-8")
                or row["manifest_hash"] != content_hash(expected)):
                raise DomainError("STAGED_RELEASE_TAMPERED")
        except (ValueError, TypeError, KeyError) as exc:
            raise DomainError("STAGED_RELEASE_TAMPERED") from exc
        return StagedRelease(
            cast(str, row["staged_release_id"]), candidate_id,
            expected, content_hash(expected),
        )

    def stage_research_draft(self, candidate_id: str) -> StagedRelease:
        """Store immutable provenance, NOT a production Release Manifest."""
        if not isinstance(candidate_id, str) or not candidate_id:
            raise DomainError("INVALID_RELEASE_CANDIDATE")
        with self.store._write() as c:
            self._active(c)
            expected = self._draft_contents(c, candidate_id)
            row = c.execute(
                "SELECT * FROM staged_release_manifests WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            if row is None:
                source = expected["source"]
                c.execute(
                    "INSERT INTO staged_release_manifests "
                    "(staged_release_id,candidate_id,baseline_release_id,"
                    "baseline_release_hash,manifest_json,manifest_hash,created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (expected["staged_release_id"], candidate_id,
                     source["baseline_release_id"], source["baseline_release_hash"],
                     canonical_bytes(expected).decode("utf-8"), content_hash(expected),
                     utc_now()),
                )
                row = c.execute(
                    "SELECT * FROM staged_release_manifests WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()
            if row is None:
                raise DomainError("STAGED_RELEASE_WRITE_FAILED")
            return self._draft_record(c, row)

    def get_draft(self, staged_release_id: str) -> StagedRelease | None:
        if not isinstance(staged_release_id, str) or not staged_release_id:
            raise DomainError("INVALID_STAGED_RELEASE_ID")
        with closing(self.store._connect()) as c:
            row = c.execute(
                "SELECT * FROM staged_release_manifests WHERE staged_release_id=?",
                (staged_release_id,),
            ).fetchone()
            return None if row is None else self._draft_record(c, row)

    @staticmethod
    def _attempt_record(c: sqlite3.Connection, row: sqlite3.Row) -> ActivationAttempt:
        request_key = cast(str, row["request_key"])
        stage_id = cast(str, row["staged_release_id"])
        expected = {
            "schema_version": 1,
            "request_key": request_key,
            "staged_release_id": stage_id,
            "expected_release_id": row["expected_release_id"],
            "expected_revision": row["expected_revision"],
            "status": "BLOCKED",
            "reason": "NO_PUBLISH_AUTHORITY_OR_CANDIDATE_EFFECT_EVIDENCE",
            "active_release_modified": False,
        }
        try:
            document = json.loads(cast(str, row["attempt_json"]))
            if (not isinstance(document, dict) or canonical_bytes(document) != canonical_bytes(expected)
                or row["attempt_json"] != canonical_bytes(expected).decode("utf-8")
                or row["attempt_hash"] != content_hash(expected)):
                raise DomainError("RELEASE_ACTIVATION_AUDIT_TAMPERED")
        except (ValueError, TypeError, KeyError) as exc:
            raise DomainError("RELEASE_ACTIVATION_AUDIT_TAMPERED") from exc
        return ActivationAttempt(
            cast(str, row["attempt_id"]), request_key, stage_id,
            "BLOCKED", cast(str, expected["reason"]), content_hash(expected),
        )

    def request_activation(
        self, staged_release_id: str, *, expected: ReleasePointer, request_key: str,
    ) -> ActivationAttempt:
        """Check the CAS precondition and durably DENY activation; no pointer write."""
        if (not isinstance(staged_release_id, str) or not staged_release_id
            or not isinstance(expected, ReleasePointer) or not isinstance(request_key, str)
            or not 1 <= len(request_key) <= 120 or not request_key.isascii()
            or not all(c.isalnum() or c in "_-" for c in request_key)):
            raise DomainError("INVALID_ACTIVATION_REQUEST")
        with self.store._write() as c:
            pointer = self._active(c)
            if pointer != expected:
                raise DomainError("STALE_RELEASE_POINTER")
            draft = c.execute(
                "SELECT * FROM staged_release_manifests WHERE staged_release_id=?",
                (staged_release_id,),
            ).fetchone()
            if draft is None:
                raise DomainError("STAGED_RELEASE_NOT_FOUND")
            self._draft_record(c, draft)
            prior = c.execute(
                "SELECT * FROM release_activation_attempts WHERE request_key=?",
                (request_key,),
            ).fetchone()
            if prior is not None:
                record = self._attempt_record(c, prior)
                if (record.staged_release_id != staged_release_id
                    or prior["expected_release_id"] != expected.release_id
                    or prior["expected_revision"] != expected.revision):
                    raise DomainError("RELEASE_ACTIVATION_IDEMPOTENCY_CONFLICT")
                return record
            payload = {
                "schema_version": 1,
                "request_key": request_key,
                "staged_release_id": staged_release_id,
                "expected_release_id": expected.release_id,
                "expected_revision": expected.revision,
                "status": "BLOCKED",
                "reason": "NO_PUBLISH_AUTHORITY_OR_CANDIDATE_EFFECT_EVIDENCE",
                "active_release_modified": False,
            }
            c.execute(
                "INSERT INTO release_activation_attempts "
                "(attempt_id,request_key,staged_release_id,expected_release_id,"
                "expected_revision,attempt_json,attempt_hash,attempted_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (uid(), request_key, staged_release_id, expected.release_id,
                 expected.revision, canonical_bytes(payload).decode("utf-8"),
                 content_hash(payload), utc_now()),
            )
            row = c.execute(
                "SELECT * FROM release_activation_attempts WHERE request_key=?",
                (request_key,),
            ).fetchone()
            if row is None:
                raise DomainError("RELEASE_ACTIVATION_AUDIT_WRITE_FAILED")
            return self._attempt_record(c, row)
