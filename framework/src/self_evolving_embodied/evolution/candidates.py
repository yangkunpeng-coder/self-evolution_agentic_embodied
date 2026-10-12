"""R08: deterministic, non-executable *review proposals* from R07 diagnoses.

No model inference, arbitrary code generation, prompt installation, capability
expansion, runtime changes, or release publishing is performed here.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError
from ..core.ids import uid, utc_now
from ..diagnosis import DiagnosisCategory, DiagnosisRecord, FailureDiagnoser
from ..storage.sqlite.database import SqliteStore


class EvolutionTarget(StrEnum):
    PLANNING_PROMPT = "PLANNING_PROMPT"
    SKILL_ROUTING = "SKILL_ROUTING"
    WORKFLOW = "WORKFLOW"


@dataclass(frozen=True)
class CandidateRecord:
    candidate_id: str
    diagnosis_id: str
    task_id: str
    baseline_release_id: str
    target: EvolutionTarget
    template_id: str
    content: dict[str, Any]
    content_hash: str


# Deliberately fixed mappings. A runtime failure does not establish its physical
# root cause, and in particular MUST NOT cause widening tool permissions.
_TEMPLATES: dict[DiagnosisCategory, tuple[EvolutionTarget, str, str]] = {
    DiagnosisCategory.GOAL_VERIFICATION_REJECTED: (
        EvolutionTarget.PLANNING_PROMPT, "GOAL_CRITERIA_REVIEW_V1",
        ("Propose a review of task decomposition against the *existing* goal predicate; "
        "do not weaken or replace verification.")),
    DiagnosisCategory.STEP_VERIFICATION_REJECTED: (
        EvolutionTarget.SKILL_ROUTING, "EVIDENCE_AVAILABILITY_REVIEW_V1",
        ("Review skill selection for ability to provide the already required step evidence; "
        "do not add or execute a skill.")),
    DiagnosisCategory.AUTHORIZATION_REJECTED: (
        EvolutionTarget.WORKFLOW, "PRE_AUTHORIZATION_CHECK_V1",
        "Consider checking the allowlist earlier in planning; do not grant permissions."),
    DiagnosisCategory.OBSERVATION_UNAVAILABLE: (
        EvolutionTarget.WORKFLOW, "FRESH_OBSERVATION_PRECHECK_V1",
        ("Review whether observation availability can be checked before dispatch; "
        "do not fabricate observations or suppress failures.")),
    DiagnosisCategory.PRE_DISPATCH_ABORT: (
        EvolutionTarget.WORKFLOW, "PRE_DISPATCH_VALIDATION_REVIEW_V1",
        "Review pre-dispatch readiness checks while retaining authorization and stop gates."),
}


class EvolutionCandidateGenerator:
    """Offline, idempotent, release-scoped proposal registry.

    One terminal diagnosis => one auditable decision (proposal or skipped).
    Proposals are *descriptions for subsequent independent evaluation*;
    neither code nor runtime configuration is changed by this class.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _payload(record: DiagnosisRecord, baseline_hash: str) -> dict[str, Any] | None:
        # R07 already checks provenance; a diagnosis must never authorize action.
        body = record.content
        if (body.get("root_cause") != "NOT_ESTABLISHED"
            or body.get("classification_basis") != "DETERMINISTIC_RECORDED_SYMPTOM"
            or body.get("auto_publish_authorized") is not False
            or body.get("automated_recovery_authorized") is not False
            or body.get("release_id") != record.release_id
            or body.get("source_experience_hash") != record.source_experience_hash):
            raise DomainError("CANDIDATE_DIAGNOSIS_INVALID")
        if body.get("source_trust") == "UNCERTAIN" or body.get("requires_human_review") is True:
            return None
        if body.get("source_trust") != "UNVERIFIED":
            raise DomainError("CANDIDATE_UNTRUSTED_SOURCE")
        template = _TEMPLATES.get(record.category)
        if template is None:
            return None
        target, template_id, review_text = template
        # No LLM-sourced content, executable patch or serialized arbitrary
        # task/action arguments. Only predetermined review descriptions.
        return {
            "schema_version": 1,
            "candidate_kind": "REVIEW_ONLY_NON_EXECUTABLE",
            "status": "PROPOSED_NOT_EVALUATED",
            "baseline_release_id": record.release_id,
            "baseline_release_hash": baseline_hash,
            "task_id": record.task_id,
            "diagnosis_id": record.diagnosis_id,
            "source_diagnosis_hash": record.content_hash,
            "source_experience_hash": record.source_experience_hash,
            "diagnosis_category": record.category.value,
            "root_cause": "NOT_ESTABLISHED",
            "target": target.value,
            "template_id": template_id,
            "suggested_review": review_text,
            "evidence_refs": body.get("evidence_refs"),
            "evaluation_status": "NOT_EVALUATED",
            "approval_status": "NOT_APPROVED",
            "apply_authorized": False,
            "publish_authorized": False,
            "modifies_live_runtime": False,
            "requires_independent_heldout_evaluation": True,
            "requires_human_approval": True,
            "excluded_mutations": [
                "SAFETY_POLICY", "VERIFIER", "ROBOT_CONTROLLER", "VLA_WEIGHTS",
                "WAM_WEIGHTS", "CAPABILITY_ALLOWLIST", "ACTIVE_RELEASE",
            ],
            "limitations": ["FAKE_ONLY", "NO_PHYSICAL_CAUSAL_INFERENCE", "NO_AUTOMATIC_PATCH"],
        }

    @staticmethod
    def _diagnosis(c: sqlite3.Connection, diagnosis_id: str) -> DiagnosisRecord:
        row = c.execute("SELECT * FROM diagnosis_records WHERE diagnosis_id=?",
                        (diagnosis_id,)).fetchone()
        if row is None:
            raise DomainError("CANDIDATE_DIAGNOSIS_MISSING")
        return FailureDiagnoser._record(c, row)

    @staticmethod
    def _baseline(c: sqlite3.Connection, release_id: str) -> str:
        row = c.execute("SELECT body,body_hash FROM release_manifests WHERE release_id=?",
                        (release_id,)).fetchone()
        if row is None:
            raise DomainError("CANDIDATE_BASELINE_MISSING")
        try:
            body = json.loads(cast(str, row["body"]))
        except (TypeError, ValueError) as exc:
            raise DomainError("CANDIDATE_BASELINE_TAMPERED") from exc
        if not isinstance(body, dict) or content_hash(body) != row["body_hash"]:
            raise DomainError("CANDIDATE_BASELINE_TAMPERED")
        return cast(str, row["body_hash"])

    def generate_pending(self, max_items: int = 100) -> int:
        """Store auditable decisions; return number of new review-only proposals.

        An invalid source aborts the complete batch transaction. Only R07
        persisted diagnoses are admitted; no inferred or text-only evidence.
        """
        if type(max_items) is not int or not 1 <= max_items <= 1000:
            raise DomainError("INVALID_CANDIDATE_BATCH")
        count = 0
        with self.store._write() as c:
            rows = c.execute(
                "SELECT d.diagnosis_id FROM diagnosis_records AS d "
                "WHERE NOT EXISTS (SELECT 1 FROM candidate_decisions AS cd "
                "WHERE cd.diagnosis_id=d.diagnosis_id) "
                "ORDER BY d.diagnosed_at,d.task_id LIMIT ?", (max_items,),
            ).fetchall()
            for row in rows:
                diagnosis = self._diagnosis(c, cast(str, row["diagnosis_id"]))
                baseline_hash = self._baseline(c, diagnosis.release_id)
                payload = self._payload(diagnosis, baseline_hash)
                if payload is None:
                    decision = "SKIPPED_UNSAFE_OR_UNSUPPORTED"
                else:
                    decision = "PROPOSED"
                    c.execute(
                        "INSERT INTO evolution_candidates "
                        "(candidate_id,diagnosis_id,task_id,baseline_release_id,"
                        "target,template_id,content_json,content_hash,created_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (uid(), diagnosis.diagnosis_id, diagnosis.task_id,
                         diagnosis.release_id, payload["target"], payload["template_id"],
                         canonical_bytes(payload).decode("utf-8"), content_hash(payload),
                         utc_now()),
                    )
                    count += 1
                c.execute(
                    "INSERT INTO candidate_decisions "
                    "(diagnosis_id,source_diagnosis_hash,decision,decided_at) VALUES (?,?,?,?)",
                    (diagnosis.diagnosis_id, diagnosis.content_hash, decision, utc_now()),
                )
        return count

    @classmethod
    def _record(cls, c: sqlite3.Connection, row: sqlite3.Row) -> CandidateRecord:
        try:
            body = json.loads(cast(str, row["content_json"]))
            if not isinstance(body, dict):
                raise DomainError("CANDIDATE_RECORD_TAMPERED")
            diagnosis = cls._diagnosis(c, cast(str, row["diagnosis_id"]))
            baseline_hash = cls._baseline(c, diagnosis.release_id)
            expected = cls._payload(diagnosis, baseline_hash)
            if (expected is None or canonical_bytes(expected) != canonical_bytes(body)
                or content_hash(body) != row["content_hash"]
                or row["task_id"] != diagnosis.task_id
                or row["baseline_release_id"] != diagnosis.release_id
                or row["target"] != body["target"]
                or row["template_id"] != body["template_id"]):
                raise DomainError("CANDIDATE_RECORD_TAMPERED")
            dec = c.execute(
                "SELECT * FROM candidate_decisions WHERE diagnosis_id=?",
                (diagnosis.diagnosis_id,),
            ).fetchone()
            if (dec is None or dec["decision"] != "PROPOSED"
                or dec["source_diagnosis_hash"] != diagnosis.content_hash):
                raise DomainError("CANDIDATE_DECISION_MISMATCH")
            return CandidateRecord(
                cast(str, row["candidate_id"]), cast(str, row["diagnosis_id"]),
                cast(str, row["task_id"]), cast(str, row["baseline_release_id"]),
                EvolutionTarget(row["target"]), cast(str, row["template_id"]),
                cast(dict[str, Any], body), cast(str, row["content_hash"]),
            )
        except (TypeError, ValueError, KeyError) as exc:
            raise DomainError("CANDIDATE_RECORD_TAMPERED") from exc

    def get(self, task_id: str, *, release_id: str) -> CandidateRecord | None:
        if not isinstance(task_id, str) or not isinstance(release_id, str) or not release_id:
            raise DomainError("INVALID_CANDIDATE_QUERY")
        with closing(self.store._connect()) as c:
            row = c.execute(
                "SELECT * FROM evolution_candidates WHERE task_id=? AND baseline_release_id=?",
                (task_id, release_id),
            ).fetchone()
            return None if row is None else self._record(c, row)

    def search(self, *, release_id: str, target: EvolutionTarget | None = None,
               limit: int = 100) -> list[CandidateRecord]:
        if (not isinstance(release_id, str) or not release_id
            or type(limit) is not int or not 1 <= limit <= 1000):
            raise DomainError("INVALID_CANDIDATE_QUERY")
        if target is not None and not isinstance(target, EvolutionTarget):
            raise DomainError("INVALID_CANDIDATE_TARGET")
        with closing(self.store._connect()) as c:
            if target is None:
                rows = c.execute(
                    "SELECT * FROM evolution_candidates WHERE baseline_release_id=? "
                    "ORDER BY created_at,task_id LIMIT ?", (release_id, limit),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM evolution_candidates "
                    "WHERE baseline_release_id=? AND target=? "
                    "ORDER BY created_at,task_id LIMIT ?", (release_id, target.value, limit),
                ).fetchall()
            return [self._record(c, row) for row in rows]
