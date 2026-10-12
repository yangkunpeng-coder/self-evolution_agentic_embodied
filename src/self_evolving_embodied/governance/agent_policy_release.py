"""Trusted-human-reviewed OFFLINE RESEARCH policy releases and rollback (v16).

This module never touches legacy active_release, release_pointer_state,
ExecutionGrant, device adapters or the live LLM planner. A research policy is
an allowlisted *declarative artifact*, NOT an executable deployment. Reading
it into a live system requires separate, future trusted integration and checks.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError
from ..core.ids import uid, utc_now
from ..evaluation.agent_policy_study import (
    IndependentPolicyEvaluator, PolicyProfile, PolicyStudyRecord, _ident, _load_object,
)
from ..review.service import ReviewerAuthority
from ..storage.sqlite.database import SqliteStore


@dataclass(frozen=True)
class ResearchReview:
    decision_id: str
    study_id: str
    reviewer_id: str
    choice: str
    body_hash: str


@dataclass(frozen=True)
class ResearchPolicyPointer:
    target: str
    revision: int
    active_policy_hash: str | None
    active_study_id: str | None
    event_head_hash: str
    policy: PolicyProfile | None
    scope: str = "OFFLINE_RESEARCH_ONLY"


class ResearchPolicyGovernance:
    """Append-only research policy log; CAS revisions and per-target hash chain.

    A human identity verifier MUST be injected. Credential values are never
    written to SQLite, and no reviewer credentials are accepted from an LLM.
    """

    def __init__(self, store: SqliteStore, authority: ReviewerAuthority | None = None) -> None:
        self.store = store
        self.authority = authority

    @staticmethod
    def _valid_reason(reason: str) -> bool:
        return (isinstance(reason, str) and 12 <= len(reason) <= 500
                and reason == reason.strip() and not any(ord(x) < 32 for x in reason))

    def _require_human(self, reviewer_id: str, credential: str) -> None:
        _ident(reviewer_id, "RESEARCH_REVIEWER_INVALID")
        if not isinstance(credential, str) or not credential:
            raise DomainError("RESEARCH_HUMAN_CREDENTIAL_REQUIRED")
        if self.authority is None:
            raise DomainError("RESEARCH_HUMAN_AUTHORITY_REQUIRED")
        try:
            approved = self.authority.verify(reviewer_id, credential)
        except Exception as exc:
            raise DomainError("RESEARCH_AUTHORITY_UNAVAILABLE") from exc
        if approved is not True:
            raise DomainError("RESEARCH_REVIEWER_UNAUTHORIZED")

    @staticmethod
    def _study(c: sqlite3.Connection, study_id: str) -> PolicyStudyRecord:
        return IndependentPolicyEvaluator._read_study(c, study_id)

    @classmethod
    def _read_review(cls, c: sqlite3.Connection, study_id: str) -> ResearchReview:
        row = c.execute("SELECT * FROM agent_policy_reviews WHERE study_id=?", (study_id,)).fetchone()
        if row is None:
            raise DomainError("RESEARCH_REVIEW_REQUIRED")
        study = cls._study(c, study_id)
        payload = _load_object(row["decision_json"], row["decision_hash"], "RESEARCH_REVIEW_TAMPERED")
        if (row["choice"] not in {"APPROVE_RESEARCH", "REJECT"}
            or payload != {"schema_version": 1, "scope": "OFFLINE_RESEARCH_ONLY",
                           "study_id": study_id, "study_hash": study.report_hash,
                           "reviewer_id": row["reviewer_id"], "choice": row["choice"],
                           "rationale": payload.get("rationale"),
                           "production_release_authorized": False}
            or not cls._valid_reason(payload["rationale"])):
            raise DomainError("RESEARCH_REVIEW_TAMPERED")
        if (row["choice"] == "APPROVE_RESEARCH" and study.report["gate"] != "PASS"):
            raise DomainError("RESEARCH_REVIEW_GATE_INCONSISTENT")
        return ResearchReview(row["decision_id"], study_id, row["reviewer_id"],
                              row["choice"], row["decision_hash"])

    def review(self, study_id: str, *, reviewer_id: str, credential: str,
               choice: str, rationale: str) -> ResearchReview:
        self._require_human(reviewer_id, credential)
        if choice not in {"APPROVE_RESEARCH", "REJECT"} or not self._valid_reason(rationale):
            raise DomainError("RESEARCH_REVIEW_INVALID")
        with self.store._write() as c:
            study = self._study(c, study_id)
            if study.report["gate"] != "PASS" and choice == "APPROVE_RESEARCH":
                raise DomainError("RESEARCH_STUDY_GATE_BLOCKED")
            if reviewer_id == study.report["operator_id"]:
                raise DomainError("RESEARCH_REVIEW_REQUIRES_SEPARATE_REVIEWER")
            row = c.execute("SELECT 1 FROM agent_policy_reviews WHERE study_id=?", (study_id,)).fetchone()
            if row:
                raise DomainError("RESEARCH_REVIEW_ALREADY_RECORDED")
            payload = {"schema_version": 1, "scope": "OFFLINE_RESEARCH_ONLY",
                       "study_id": study_id, "study_hash": study.report_hash,
                       "reviewer_id": reviewer_id, "choice": choice, "rationale": rationale,
                       "production_release_authorized": False}
            c.execute("INSERT INTO agent_policy_reviews VALUES (?,?,?,?,?,?,?)",
                      (uid(), study_id, reviewer_id, choice,
                       canonical_bytes(payload).decode("utf-8"), content_hash(payload), utc_now()))
            return self._read_review(c, study_id)

    @staticmethod
    def _policy_from_study(study: PolicyStudyRecord) -> PolicyProfile:
        raw = study.report["candidate_policy"]
        try:
            if (not isinstance(raw, dict) or set(raw) != {"schema_version", "scope", "target", "settings"}
                or raw["schema_version"] != 1 or raw["scope"] != "OFFLINE_RESEARCH_ONLY"):
                raise DomainError("RESEARCH_POLICY_INVALID")
            config = PolicyProfile(raw["target"], raw["settings"])
        except (ValueError, KeyError, TypeError) as exc:
            raise DomainError("RESEARCH_POLICY_INVALID") from exc
        if config.digest != study.report["candidate_hash"]:
            raise DomainError("RESEARCH_POLICY_HASH_MISMATCH")
        return config

    @classmethod
    def _pointer(cls, c: sqlite3.Connection, target: str) -> ResearchPolicyPointer:
        _ident(target, "RESEARCH_POLICY_TARGET_INVALID")
        from ..evolution.workflow_cycle import EvolutionPolicyTarget
        try:
            EvolutionPolicyTarget(target)
        except ValueError as exc:
            raise DomainError("RESEARCH_POLICY_TARGET_INVALID") from exc
        rows = c.execute("SELECT * FROM agent_policy_release_events WHERE target=? ORDER BY revision",
                         (target,)).fetchall()
        previous_hash = "GENESIS"
        history: list[tuple[str | None, str | None]] = [(None, None)]
        for idx, row in enumerate(rows, 1):
            body = _load_object(row["body_json"], row["body_hash"], "RESEARCH_RELEASE_TAMPERED")
            if (row["revision"] != idx or row["previous_hash"] != previous_hash
                or row["operation"] not in {"PROMOTE", "ROLLBACK"}
                or row["operation"] != body.get("operation")
                or body.get("schema_version") != 1
                or body.get("scope") != "OFFLINE_RESEARCH_ONLY"
                or body.get("target") != target
                or body.get("revision") != idx
                or body.get("previous_event_hash") != previous_hash
                or body.get("policy_hash") != row["policy_hash"]
                or body.get("study_id") != row["study_id"]
                or body.get("production_release_authorized") is not False):
                raise DomainError("RESEARCH_RELEASE_TAMPERED")
            prior_policy, prior_study = history[-1]
            if body.get("previous_active_hash") != prior_policy:
                raise DomainError("RESEARCH_RELEASE_CHAIN_INVALID")
            if row["operation"] == "PROMOTE":
                study = cls._study(c, row["study_id"])
                review = cls._read_review(c, row["study_id"])
                config = cls._policy_from_study(study)
                if (review.choice != "APPROVE_RESEARCH" or study.report["gate"] != "PASS"
                    or config.target != target or config.digest != row["policy_hash"]
                    or body.get("review_hash") != review.body_hash
                    or row["policy_hash"] == prior_policy):
                    raise DomainError("RESEARCH_RELEASE_INVALID_APPROVAL")
            else:
                # Rollback means exactly the state one release before current.
                # Immutable history is never deleted or rewritten.
                if len(history) < 2 or (row["policy_hash"], row["study_id"]) != history[-2]:
                    raise DomainError("RESEARCH_ROLLBACK_TARGET_INVALID")
                if (not isinstance(body.get("reviewer_id"), str)
                    or not cls._valid_reason(body.get("rationale"))):
                    raise DomainError("RESEARCH_ROLLBACK_RECORD_INVALID")
            history.append((row["policy_hash"], row["study_id"]))
            previous_hash = row["body_hash"]
        current_policy, current_study = history[-1]
        config = None
        if current_study is not None:
            study = cls._study(c, current_study)
            config = cls._policy_from_study(study)
            if config.digest != current_policy:
                raise DomainError("RESEARCH_RELEASE_POLICY_INVALID")
        return ResearchPolicyPointer(target, len(rows), current_policy, current_study,
                                     previous_hash, config)

    def pointer(self, target: str) -> ResearchPolicyPointer:
        with closing(self.store._connect()) as c:
            return self._pointer(c, target)

    def promote(self, study_id: str, *, reviewer_id: str, credential: str,
                expected_revision: int) -> ResearchPolicyPointer:
        self._require_human(reviewer_id, credential)
        if type(expected_revision) is not int or expected_revision < 0:
            raise DomainError("RESEARCH_REVISION_INVALID")
        with self.store._write() as c:
            study = self._study(c, study_id)
            review = self._read_review(c, study_id)
            if study.report["gate"] != "PASS" or review.choice != "APPROVE_RESEARCH":
                raise DomainError("RESEARCH_RELEASE_NOT_APPROVED")
            config = self._policy_from_study(study)
            current = self._pointer(c, config.target)
            if current.revision != expected_revision:
                raise DomainError("RESEARCH_RELEASE_STALE_REVISION")
            if current.active_policy_hash == config.digest:
                raise DomainError("RESEARCH_RELEASE_ALREADY_ACTIVE")
            body = {"schema_version": 1, "scope": "OFFLINE_RESEARCH_ONLY",
                    "target": config.target, "revision": current.revision + 1,
                    "operation": "PROMOTE", "study_id": study_id,
                    "policy_hash": config.digest, "review_hash": review.body_hash,
                    "previous_active_hash": current.active_policy_hash,
                    "previous_event_hash": current.event_head_hash,
                    "production_release_authorized": False}
            c.execute("INSERT INTO agent_policy_release_events VALUES (?,?,?,?,?,?,?,?,?,?)",
                      (uid(), config.target, current.revision + 1, "PROMOTE", config.digest,
                       study_id, current.event_head_hash,
                       canonical_bytes(body).decode("utf-8"), content_hash(body), utc_now()))
            return self._pointer(c, config.target)

    def rollback(self, target: str, *, reviewer_id: str, credential: str,
                 rationale: str, expected_revision: int) -> ResearchPolicyPointer:
        self._require_human(reviewer_id, credential)
        if type(expected_revision) is not int or expected_revision < 1 or not self._valid_reason(rationale):
            raise DomainError("RESEARCH_ROLLBACK_INVALID")
        with self.store._write() as c:
            current = self._pointer(c, target)
            if current.revision != expected_revision:
                raise DomainError("RESEARCH_RELEASE_STALE_REVISION")
            rows = c.execute("SELECT policy_hash,study_id FROM agent_policy_release_events "
                             "WHERE target=? ORDER BY revision DESC LIMIT 2", (target,)).fetchall()
            previous_policy = rows[1]["policy_hash"] if len(rows) >= 2 else None
            previous_study = rows[1]["study_id"] if len(rows) >= 2 else None
            if previous_policy == current.active_policy_hash:
                raise DomainError("RESEARCH_ROLLBACK_NO_CHANGE")
            body = {"schema_version": 1, "scope": "OFFLINE_RESEARCH_ONLY",
                    "target": target, "revision": current.revision + 1,
                    "operation": "ROLLBACK", "study_id": previous_study,
                    "policy_hash": previous_policy, "previous_active_hash": current.active_policy_hash,
                    "previous_event_hash": current.event_head_hash,
                    "reviewer_id": reviewer_id, "rationale": rationale,
                    "production_release_authorized": False}
            c.execute("INSERT INTO agent_policy_release_events VALUES (?,?,?,?,?,?,?,?,?,?)",
                      (uid(), target, current.revision + 1, "ROLLBACK", previous_policy,
                       previous_study, current.event_head_hash,
                       canonical_bytes(body).decode("utf-8"), content_hash(body), utc_now()))
            return self._pointer(c, target)
