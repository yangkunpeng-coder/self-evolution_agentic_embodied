"""R07: evidence-scoped *symptom* diagnosis, never speculative root-cause attribution.

No model calls, robot actions, prompt/skill edits, policy bypass, or automatic
candidate generation. This module only reads durable R06 experiences and
projects immutable, reviewable diagnoses for FAILED or HUMAN_REVIEW tasks.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError, Status
from ..core.ids import uid, utc_now
from ..experience.collector import ExperienceCollector, ExperienceRecord, ExperienceTrust
from ..storage.sqlite.database import SqliteStore


class DiagnosisCategory(StrEnum):
    EXECUTION_UNCERTAIN = "EXECUTION_UNCERTAIN"
    GOAL_VERIFICATION_REJECTED = "GOAL_VERIFICATION_REJECTED"
    STEP_VERIFICATION_REJECTED = "STEP_VERIFICATION_REJECTED"
    AUTHORIZATION_REJECTED = "AUTHORIZATION_REJECTED"
    OBSERVATION_UNAVAILABLE = "OBSERVATION_UNAVAILABLE"
    PRE_DISPATCH_ABORT = "PRE_DISPATCH_ABORT"
    RECORDED_FAILURE_OTHER = "RECORDED_FAILURE_OTHER"


@dataclass(frozen=True)
class DiagnosisRecord:
    diagnosis_id: str
    experience_id: str
    task_id: str
    release_id: str
    category: DiagnosisCategory
    source_experience_hash: str
    content: dict[str, Any]
    content_hash: str


class FailureDiagnoser:
    """Convert durable failure records into bounded classifications, not root causes.

    Diagnosis is an offline SQLite projection, not part of the action loop.
    A batch is atomic, idempotent, worker-fenced, and safe to retry after restart.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _classify(episode: ExperienceRecord) -> DiagnosisCategory:
        body = episode.content
        terminal = body.get("terminal_event")
        if not isinstance(terminal, dict):
            raise DomainError("DIAGNOSIS_SOURCE_INVALID")
        reason = terminal.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise DomainError("DIAGNOSIS_SOURCE_INVALID")
        action = body.get("action")
        if episode.terminal_status is Status.HUMAN_REVIEW:
            if episode.trust_level is not ExperienceTrust.UNCERTAIN:
                raise DomainError("DIAGNOSIS_TRUST_MISMATCH")
            return DiagnosisCategory.EXECUTION_UNCERTAIN
        if episode.terminal_status is not Status.FAILED:
            raise DomainError("DIAGNOSIS_NON_FAILURE")
        if episode.trust_level is not ExperienceTrust.UNVERIFIED:
            raise DomainError("DIAGNOSIS_TRUST_MISMATCH")
        # These codes are emitted by Runtime/Verifier; they are NOT inferred
        # diagnoses of perception, physics, control or equipment root causes.
        if isinstance(reason, str) and reason.startswith("GOAL_"):
            return DiagnosisCategory.GOAL_VERIFICATION_REJECTED
        if isinstance(reason, str) and reason.startswith("STEP_"):
            return DiagnosisCategory.STEP_VERIFICATION_REJECTED
        if reason in {"UNAUTHORIZED_TOOL", "SKILL_UNAVAILABLE", "CAPABILITY_DENIED",
                      "SKILL_NOT_PUBLISHED", "UNAPPROVED_TOOL", "UNKNOWN_TOOL",
                      "SKILL_NOT_FOUND", "PARAMETERS_DENIED"}:
            return DiagnosisCategory.AUTHORIZATION_REJECTED
        if reason in {"NO_PRE_OBSERVATION", "OBSERVATION_UNAVAILABLE"}:
            return DiagnosisCategory.OBSERVATION_UNAVAILABLE
        if (reason == "INTERRUPTED_BEFORE_INTENT"
            or (action is None and reason not in {None, ""})):
            return DiagnosisCategory.PRE_DISPATCH_ABORT
        return DiagnosisCategory.RECORDED_FAILURE_OTHER

    @classmethod
    def _payload(cls, episode: ExperienceRecord) -> dict[str, Any]:
        body = episode.content
        category = cls._classify(episode)
        terminal = body["terminal_event"]
        action = body.get("action")
        evidence = body.get("observation_action_feedback")
        if (not isinstance(evidence, list)
            or (action is not None and not isinstance(action, dict))):
            raise DomainError("DIAGNOSIS_SOURCE_INVALID")
        refs: list[dict[str, str]] = [{"kind": "EXPERIENCE",
                                      "id": episode.experience_id,
                                      "hash": episode.content_hash},
                                     {"kind": "TASK_EVENT_HEAD",
                                      "id": episode.task_id,
                                      "hash": episode.event_head_hash}]
        for entry in evidence:
            if not isinstance(entry, dict):
                raise DomainError("DIAGNOSIS_SOURCE_INVALID")
            if not isinstance(entry.get("evidence_id"), str):
                raise DomainError("DIAGNOSIS_SOURCE_INVALID")
            if not isinstance(entry.get("evidence_hash"), str):
                raise DomainError("DIAGNOSIS_SOURCE_INVALID")
            refs.append({"kind": "EVIDENCE", "id": entry["evidence_id"],
                         "hash": entry["evidence_hash"]})
        # `reason` is a Runtime/event code or Stop ACK, NOT an established cause.
        reason = terminal.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise DomainError("DIAGNOSIS_SOURCE_INVALID")
        summary = {
            "terminal_event_kind": terminal.get("kind"),
            "terminal_event_reason": reason,
            "invocation_id": action.get("invocation_id") if action else None,
            "tool_id": action.get("tool_id") if action else None,
            "resource": action.get("resource") if action else None,
            "execution_status": action.get("execution_status") if action else None,
            "provider_status": action.get("provider_status") if action else None,
            "verdicts": [{"scope": item.get("scope"),
                          "result": item.get("verdict_result"),
                          "condition_id": item.get("condition_id"),
                          "after_predicate_satisfied": (
                              item["after"].get("predicate_satisfied")
                              if isinstance(item.get("after"), dict) else None)}
                         for item in evidence],
        }
        # Future candidate generation MUST treat this as a diagnostic symptom
        # requiring independent evaluation, not as permission to change skills.
        return {
            "schema_version": 1,
            "task_id": episode.task_id,
            "release_id": episode.release_id,
            "experience_id": episode.experience_id,
            "source_experience_hash": episode.content_hash,
            "source_event_head_hash": episode.event_head_hash,
            "source_trust": episode.trust_level.value,
            "terminal_status": episode.terminal_status.value,
            "category": category.value,
            "classification_basis": "DETERMINISTIC_RECORDED_SYMPTOM",
            "root_cause": "NOT_ESTABLISHED",
            "physical_outcome": "UNKNOWN" if episode.trust_level is ExperienceTrust.UNCERTAIN
            else "NOT_INFERRED_FROM_RUNTIME_STATUS",
            "signals": summary,
            "evidence_refs": refs,
            "automated_recovery_authorized": False,
            "auto_publish_authorized": False,
            "requires_human_review": episode.trust_level is ExperienceTrust.UNCERTAIN,
            "limitations": ["FAKE_ADAPTER_ONLY", "ONE_STEP_ONLY", "NO_RAW_SENSOR_ASSETS",
                            "NO_PHYSICAL_ROOT_CAUSE_INFERENCE"],
        }

    @staticmethod
    def _source(c: sqlite3.Connection, experience_id: str) -> ExperienceRecord:
        row = c.execute("SELECT * FROM experience_records WHERE experience_id=?",
                        (experience_id,)).fetchone()
        if row is None:
            raise DomainError("DIAGNOSIS_EXPERIENCE_MISSING")
        episode = ExperienceCollector._record(row)
        task = c.execute("SELECT * FROM tasks WHERE task_id=?", (episode.task_id,)).fetchone()
        if (task is None or task["release_id"] != episode.release_id
            or task["status"] != episode.terminal_status.value
            or task["last_event_hash"] != episode.event_head_hash):
            raise DomainError("DIAGNOSIS_SOURCE_INVALID")
        # R06 re-projection revalidates Event Log, Evidence, Verdict and
        # Commit provenance; any historical source drift fails closed.
        if content_hash(ExperienceCollector._project(c, task)) != episode.content_hash:
            raise DomainError("DIAGNOSIS_SOURCE_CHANGED")
        return episode

    def diagnose_pending(self, max_items: int = 100) -> int:
        """Project FAILED/HUMAN_REVIEW only; corrupt batch rolls back entirely."""
        if type(max_items) is not int or not 1 <= max_items <= 1000:
            raise DomainError("INVALID_DIAGNOSIS_BATCH")
        total = 0
        with self.store._write() as c:
            rows = c.execute(
                "SELECT e.experience_id FROM experience_records AS e "
                "WHERE e.terminal_status IN ('FAILED','HUMAN_REVIEW') "
                "AND NOT EXISTS (SELECT 1 FROM diagnosis_records AS d "
                "WHERE d.experience_id=e.experience_id) "
                "ORDER BY e.projected_at,e.task_id LIMIT ?", (max_items,),
            ).fetchall()
            for row in rows:
                episode = self._source(c, cast(str, row["experience_id"]))
                body = self._payload(episode)
                c.execute(
                    "INSERT INTO diagnosis_records VALUES (?,?,?,?,?,?,?,?,?)",
                    (uid(), episode.experience_id, episode.task_id, episode.release_id,
                     body["category"], episode.content_hash,
                     canonical_bytes(body).decode("utf-8"), content_hash(body), utc_now()),
                )
                total += 1
        return total

    @classmethod
    def _record(cls, c: sqlite3.Connection, row: sqlite3.Row) -> DiagnosisRecord:
        try:
            body = cls._json_object(cast(str, row["content_json"]))
            if (content_hash(body) != row["content_hash"]
                or body["experience_id"] != row["experience_id"]
                or body["task_id"] != row["task_id"]
                or body["release_id"] != row["release_id"]
                or body["category"] != row["category"]
                or body["source_experience_hash"] != row["source_experience_hash"]):
                raise DomainError("DIAGNOSIS_RECORD_TAMPERED")
            episode = cls._source(c, cast(str, row["experience_id"]))
            if episode.content_hash != row["source_experience_hash"]:
                raise DomainError("DIAGNOSIS_SOURCE_CHANGED")
            if canonical_bytes(cls._payload(episode)) != canonical_bytes(body):
                raise DomainError("DIAGNOSIS_RECORD_TAMPERED")
            return DiagnosisRecord(
                cast(str, row["diagnosis_id"]), cast(str, row["experience_id"]),
                cast(str, row["task_id"]), cast(str, row["release_id"]),
                DiagnosisCategory(row["category"]),
                cast(str, row["source_experience_hash"]), body,
                cast(str, row["content_hash"]),
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise DomainError("DIAGNOSIS_RECORD_TAMPERED") from exc

    @staticmethod
    def _json_object(value: str) -> dict[str, Any]:
        try:
            obj = json.loads(value)
        except (ValueError, TypeError) as exc:
            raise DomainError("DIAGNOSIS_RECORD_TAMPERED") from exc
        if not isinstance(obj, dict):
            raise DomainError("DIAGNOSIS_RECORD_TAMPERED")
        return cast(dict[str, Any], obj)

    def get(self, task_id: str, *, release_id: str) -> DiagnosisRecord | None:
        if not isinstance(task_id, str) or not isinstance(release_id, str) or not release_id:
            raise DomainError("INVALID_DIAGNOSIS_QUERY")
        with closing(self.store._connect()) as c:
            row = c.execute(
                "SELECT * FROM diagnosis_records WHERE task_id=? AND release_id=?",
                (task_id, release_id),
            ).fetchone()
            return None if row is None else self._record(c, row)

    def search(self, *, release_id: str, category: DiagnosisCategory | None = None,
               limit: int = 100) -> list[DiagnosisRecord]:
        if (not isinstance(release_id, str) or not release_id
            or type(limit) is not int or not 1 <= limit <= 1000):
            raise DomainError("INVALID_DIAGNOSIS_QUERY")
        if category is not None and not isinstance(category, DiagnosisCategory):
            raise DomainError("INVALID_DIAGNOSIS_CATEGORY")
        with closing(self.store._connect()) as c:
            if category is None:
                rows = c.execute(
                    "SELECT * FROM diagnosis_records WHERE release_id=? "
                    "ORDER BY diagnosed_at,task_id LIMIT ?", (release_id, limit),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM diagnosis_records WHERE release_id=? AND category=? "
                    "ORDER BY diagnosed_at,task_id LIMIT ?",
                    (release_id, category.value, limit),
                ).fetchall()
            return [self._record(c, row) for row in rows]
