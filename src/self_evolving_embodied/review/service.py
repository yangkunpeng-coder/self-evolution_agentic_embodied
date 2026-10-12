"""R09b review-only decisions over immutable, blocked R09a evaluations.

An accepted RESEARCH REVIEW is not an approval to publish or apply code.
No production identity provider is bundled: callers must explicitly inject a
trusted credential verifier, otherwise all decisions are rejected.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Protocol, cast

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError
from ..core.ids import uid, utc_now
from ..evaluation.evaluator import CandidateEvaluator, EvaluationRecord
from ..storage.sqlite.database import SqliteStore


class ReviewerAuthority(Protocol):
    """Trusted integration point. Must authenticate the named human independently."""

    def verify(self, reviewer_id: str, credential: str) -> bool: ...


@dataclass(frozen=True)
class ReviewRequest:
    request_id: str
    candidate_id: str
    evaluation_id: str
    content: dict[str, Any]
    content_hash: str


@dataclass(frozen=True)
class ReviewDecision:
    decision_id: str
    request_id: str
    reviewer_id: str
    choice: str
    content: dict[str, Any]
    content_hash: str


class ReviewService:
    """Append-only request/decision store, read-time provenance validation."""

    CHOICES = frozenset({"ACCEPT_FOR_RESEARCH", "REJECT"})

    def __init__(self, store: SqliteStore, authority: ReviewerAuthority | None = None) -> None:
        self.store = store
        self.authority = authority

    @staticmethod
    def _evaluation(c: sqlite3.Connection, candidate_id: str) -> EvaluationRecord:
        row = c.execute(
            "SELECT * FROM candidate_evaluations WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        if row is None:
            raise DomainError("REVIEW_EVALUATION_REQUIRED")
        evaluation = CandidateEvaluator._record(c, row)
        report = evaluation.report
        if (report.get("gate_status") != "BLOCKED"
            or report.get("promotion_eligible") is not False
            or report.get("candidate_effect_measured") is not False):
            raise DomainError("REVIEW_EVALUATION_INCOMPATIBLE")
        return evaluation

    @staticmethod
    def _payload(e: EvaluationRecord) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "candidate_id": e.candidate_id,
            "evaluation_id": e.evaluation_id,
            "evaluation_hash": e.report_hash,
            "baseline_release_id": e.baseline_release_id,
            "baseline_release_hash": e.report["baseline_release_hash"],
            "review_scope": "RESEARCH_REVIEW_ONLY",
            "evaluation_gate": "BLOCKED",
            "release_approval_allowed": False,
            "changes_active_release": False,
        }

    @classmethod
    def _request_record(cls, c: sqlite3.Connection, row: sqlite3.Row) -> ReviewRequest:
        evaluation = cls._evaluation(c, cast(str, row["candidate_id"]))
        expected = cls._payload(evaluation)
        try:
            body = json.loads(cast(str, row["request_json"]))
            if (not isinstance(body, dict)
                or canonical_bytes(body) != canonical_bytes(expected)
                or row["request_json"] != canonical_bytes(body).decode("utf-8")
                or row["request_hash"] != content_hash(expected)
                or row["evaluation_id"] != evaluation.evaluation_id
                or row["evaluation_hash"] != evaluation.report_hash
                or row["baseline_release_id"] != evaluation.baseline_release_id
                or row["baseline_release_hash"] != expected["baseline_release_hash"]):
                raise DomainError("REVIEW_REQUEST_TAMPERED")
        except (TypeError, ValueError, KeyError) as exc:
            raise DomainError("REVIEW_REQUEST_TAMPERED") from exc
        return ReviewRequest(
            cast(str, row["request_id"]), evaluation.candidate_id,
            evaluation.evaluation_id, expected, content_hash(expected),
        )

    def request(self, candidate_id: str) -> ReviewRequest:
        if not isinstance(candidate_id, str) or not candidate_id:
            raise DomainError("INVALID_REVIEW_QUERY")
        with self.store._write() as c:
            evaluation = self._evaluation(c, candidate_id)
            existing = c.execute(
                "SELECT * FROM candidate_review_requests WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            if existing is not None:
                return self._request_record(c, existing)
            payload = self._payload(evaluation)
            request_id = uid()
            c.execute(
                "INSERT INTO candidate_review_requests "
                "(request_id,candidate_id,evaluation_id,evaluation_hash,"
                "baseline_release_id,baseline_release_hash,request_json,request_hash,requested_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (request_id, candidate_id, evaluation.evaluation_id, evaluation.report_hash,
                 evaluation.baseline_release_id, payload["baseline_release_hash"],
                 canonical_bytes(payload).decode("utf-8"), content_hash(payload), utc_now()),
            )
            row = c.execute(
                "SELECT * FROM candidate_review_requests WHERE request_id=?", (request_id,),
            ).fetchone()
            if row is None:
                raise DomainError("REVIEW_REQUEST_WRITE_FAILED")
            return self._request_record(c, row)

    @staticmethod
    def _decision_payload(
        request: ReviewRequest, reviewer_id: str, choice: str, rationale: str,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "request_id": request.request_id,
            "request_hash": request.content_hash,
            "candidate_id": request.candidate_id,
            "evaluation_id": request.evaluation_id,
            "evaluation_hash": request.content["evaluation_hash"],
            "reviewer_id": reviewer_id,
            "choice": choice,
            "rationale": rationale,
            "scope": "RESEARCH_REVIEW_ONLY",
            "release_approved": False,
            "promotion_eligible": False,
            "active_release_modified": False,
        }

    @classmethod
    def _decision_record(cls, c: sqlite3.Connection, row: sqlite3.Row) -> ReviewDecision:
        request_row = c.execute(
            "SELECT * FROM candidate_review_requests WHERE request_id=?",
            (row["request_id"],),
        ).fetchone()
        if request_row is None:
            raise DomainError("REVIEW_DECISION_TAMPERED")
        request = cls._request_record(c, request_row)
        try:
            body = json.loads(cast(str, row["decision_json"]))
            if not isinstance(body, dict):
                raise DomainError("REVIEW_DECISION_TAMPERED")
            rationale = body.get("rationale")
            if not isinstance(rationale, str) or not cls._valid_rationale(rationale):
                raise DomainError("REVIEW_DECISION_TAMPERED")
            expected = cls._decision_payload(
                request, cast(str, row["reviewer_id"]), cast(str, row["choice"]), rationale,
            )
            if (row["choice"] not in cls.CHOICES
                or not cls._valid_reviewer(row["reviewer_id"])
                or canonical_bytes(body) != canonical_bytes(expected)
                or row["decision_json"] != canonical_bytes(body).decode("utf-8")
                or row["decision_hash"] != content_hash(expected)):
                raise DomainError("REVIEW_DECISION_TAMPERED")
        except (ValueError, TypeError, KeyError) as exc:
            raise DomainError("REVIEW_DECISION_TAMPERED") from exc
        return ReviewDecision(
            cast(str, row["decision_id"]), request.request_id,
            cast(str, row["reviewer_id"]), cast(str, row["choice"]),
            expected, content_hash(expected),
        )

    @staticmethod
    def _valid_reviewer(value: Any) -> bool:
        return (isinstance(value, str) and 1 <= len(value) <= 100
                and all(c.isascii() and (c.isalnum() or c in "_.@-") for c in value))

    @staticmethod
    def _valid_rationale(value: Any) -> bool:
        return (isinstance(value, str) and 8 <= len(value) <= 500
                and value == value.strip()
                and not any(ord(c) < 32 for c in value))

    def decide(
        self, request_id: str, *, reviewer_id: str, credential: str,
        choice: str, rationale: str,
    ) -> ReviewDecision:
        if (not isinstance(request_id, str) or not request_id
            or not self._valid_reviewer(reviewer_id)
            or not isinstance(choice, str) or choice not in self.CHOICES
            or not self._valid_rationale(rationale)):
            raise DomainError("INVALID_REVIEW_DECISION")
        if self.authority is None or not isinstance(credential, str) or not credential:
            raise DomainError("REVIEW_AUTHORITY_REQUIRED")
        try:
            verified = self.authority.verify(reviewer_id, credential)
        except Exception as exc:
            raise DomainError("REVIEW_AUTHORITY_UNAVAILABLE") from exc
        if verified is not True:
            raise DomainError("REVIEW_REVIEWER_UNAUTHORIZED")
        with self.store._write() as c:
            request_row = c.execute(
                "SELECT * FROM candidate_review_requests WHERE request_id=?", (request_id,),
            ).fetchone()
            if request_row is None:
                raise DomainError("REVIEW_REQUEST_NOT_FOUND")
            request = self._request_record(c, request_row)
            prior = c.execute(
                "SELECT * FROM candidate_review_decisions WHERE request_id=?", (request_id,),
            ).fetchone()
            if prior is not None:
                previous = self._decision_record(c, prior)
                expected = self._decision_payload(request, reviewer_id, choice, rationale)
                if previous.content != expected:
                    raise DomainError("REVIEW_ALREADY_DECIDED")
                return previous
            payload = self._decision_payload(request, reviewer_id, choice, rationale)
            decision_id = uid()
            c.execute(
                "INSERT INTO candidate_review_decisions "
                "(decision_id,request_id,reviewer_id,choice,decision_json,decision_hash,"
                "decided_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (decision_id, request_id, reviewer_id, choice,
                 canonical_bytes(payload).decode("utf-8"), content_hash(payload), utc_now()),
            )
            row = c.execute(
                "SELECT * FROM candidate_review_decisions WHERE decision_id=?", (decision_id,),
            ).fetchone()
            if row is None:
                raise DomainError("REVIEW_DECISION_WRITE_FAILED")
            return self._decision_record(c, row)

    def get(self, candidate_id: str) -> tuple[ReviewRequest, ReviewDecision | None] | None:
        if not isinstance(candidate_id, str) or not candidate_id:
            raise DomainError("INVALID_REVIEW_QUERY")
        with closing(self.store._connect()) as c:
            row = c.execute(
                "SELECT * FROM candidate_review_requests WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            if row is None:
                return None
            request = self._request_record(c, row)
            decision_row = c.execute(
                "SELECT * FROM candidate_review_decisions WHERE request_id=?",
                (request.request_id,),
            ).fetchone()
            decision = (None if decision_row is None
                        else self._decision_record(c, decision_row))
            return request, decision
