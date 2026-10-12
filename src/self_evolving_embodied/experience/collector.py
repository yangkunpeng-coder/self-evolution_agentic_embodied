"""R06: durable, evidence-linked experience projection. No model calls or robot actions.

Experience is a non-executable historical fact, NOT a planner instruction and
NOT an automatically approved self-evolution candidate. This MVP stores hashes,
predicate transitions and identifiers, but no raw robot images or action payloads.
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
from ..memory.store import SqliteMemory
from ..storage.sqlite.database import SqliteStore


class ExperienceTrust(StrEnum):
    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"
    UNCERTAIN = "UNCERTAIN"


@dataclass(frozen=True)
class ExperienceRecord:
    experience_id: str
    task_id: str
    release_id: str
    terminal_status: Status
    trust_level: ExperienceTrust
    content: dict[str, Any]
    content_hash: str
    event_head_hash: str


class ExperienceCollector:
    """Build exactly one immutable episode per terminal one-step Fake task.

    Projection holds one SQLite transaction, validates event and evidence
    provenance, and does not invoke an adapter or modify task/skill/memory.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _json_object(value: str, code: str) -> dict[str, Any]:
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError) as exc:
            raise DomainError(code) from exc
        if not isinstance(parsed, dict):
            raise DomainError(code)
        return cast(dict[str, Any], parsed)

    @classmethod
    def _checked_events(cls, c: sqlite3.Connection, task: sqlite3.Row) -> list[dict[str, Any]]:
        rows = c.execute(
            "SELECT seq,kind,payload,previous_hash,event_hash FROM events "
            "WHERE task_id=? ORDER BY seq", (task["task_id"],),
        ).fetchall()
        if not rows or len(rows) != task["event_seq"]:
            raise DomainError("EXPERIENCE_EVENT_CHAIN_INVALID")
        previous = ""
        events: list[dict[str, Any]] = []
        for index, row in enumerate(rows, 1):
            payload = cls._json_object(row["payload"], "EXPERIENCE_EVENT_CHAIN_INVALID")
            item = {"task_id": task["task_id"], "seq": row["seq"],
                    "kind": row["kind"], "payload": payload, "previous_hash": previous}
            if (row["seq"] != index or row["previous_hash"] != previous
                or content_hash(item) != row["event_hash"]):
                raise DomainError("EXPERIENCE_EVENT_CHAIN_INVALID")
            previous = cast(str, row["event_hash"])
            events.append(item)
        if previous != task["last_event_hash"]:
            raise DomainError("EXPERIENCE_EVENT_CHAIN_INVALID")
        terminal_kinds = {
            Status.SUCCEEDED: "COMMIT_ACCEPTED",
            Status.FAILED: "TASK_FAILED",
            Status.HUMAN_REVIEW: "EXECUTION_UNKNOWN",
        }
        actual = events[-1]["kind"]
        if actual != terminal_kinds[Status(task["status"])] and not (
            (task["status"] == Status.FAILED.value and actual == "RECOVERY_NO_DISPATCH")
            or (task["status"] == Status.HUMAN_REVIEW.value and actual == "RECOVERY_UNKNOWN")
        ):
            raise DomainError("EXPERIENCE_TERMINAL_EVENT_INVALID")
        return events

    @staticmethod
    def _predicate(state: object, condition: dict[str, Any]) -> bool | None:
        if not isinstance(state, dict):
            return None
        current: object = state
        field = condition.get("field")
        if not isinstance(field, str):
            raise DomainError("EXPERIENCE_CONDITION_INVALID")
        for key in field.split("."):
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        kind = condition.get("kind")
        if kind == "exists":
            return True
        if kind == "equals":
            expected = condition.get("expected")
            return type(current) is type(expected) and current == expected
        raise DomainError("EXPERIENCE_CONDITION_INVALID")

    @classmethod
    def _observation(cls, item: object, cond: dict[str, Any]) -> dict[str, Any] | None:
        if item is None:
            return None
        if not isinstance(item, dict) or not isinstance(item.get("state"), dict):
            raise DomainError("EXPERIENCE_OBSERVATION_INVALID")
        return {"observation_id": item.get("observation_id"),
                "observed_at": item.get("observed_at"),
                "state_hash": content_hash(item["state"]),
                "predicate_satisfied": cls._predicate(item["state"], cond)}

    @classmethod
    def _evidences(cls, c: sqlite3.Connection, task: sqlite3.Row,
                   invocation: sqlite3.Row | None) -> list[dict[str, Any]]:
        rows = c.execute(
            "SELECT e.*, v.verdict_id, v.payload AS verdict_payload, "
            "v.verdict_hash FROM evidences AS e JOIN verdicts AS v "
            "ON v.evidence_id=e.evidence_id WHERE e.task_id=? "
            "ORDER BY CASE e.scope WHEN 'STEP' THEN 0 ELSE 1 END, e.rowid",
            (task["task_id"],),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            e = cls._json_object(row["payload"], "EXPERIENCE_PROVENANCE_INVALID")
            v = cls._json_object(row["verdict_payload"], "EXPERIENCE_PROVENANCE_INVALID")
            if (content_hash(e) != row["evidence_hash"]
                or content_hash(v) != row["verdict_hash"]
                or e.get("evidence_id") != row["evidence_id"]
                or v.get("verdict_id") != row["verdict_id"]
                or v.get("evidence_hash") != row["evidence_hash"]
                or v.get("task_id") != task["task_id"]
                or e.get("task_id") != task["task_id"]
                or e.get("release_id") != task["release_id"]
                or v.get("release_id") != task["release_id"]
                or (invocation is None or e.get("invocation_id") != invocation["invocation_id"])
                or v.get("scope") != e.get("scope")
                or row["scope"] != e.get("scope")):
                raise DomainError("EXPERIENCE_PROVENANCE_INVALID")
            cond_json = (task["goal_json"] if e["scope"] == "GOAL"
                         else invocation["step_condition_json"])
            cond = cls._json_object(cond_json, "EXPERIENCE_CONDITION_INVALID")
            result.append({"scope": e["scope"], "evidence_id": row["evidence_id"],
                           "evidence_hash": row["evidence_hash"],
                           "verdict_id": row["verdict_id"],
                           "verdict_result": v.get("result"),
                           "condition_id": v.get("condition_id"),
                           "before": cls._observation(e.get("before"), cond),
                           "after": cls._observation(e.get("after"), cond)})
        return result

    @classmethod
    def _project(cls, c: sqlite3.Connection, task: sqlite3.Row) -> dict[str, Any]:
        status = Status(task["status"])
        events = cls._checked_events(c, task)
        invocation_rows = c.execute(
            "SELECT * FROM invocations WHERE task_id=? ORDER BY rowid", (task["task_id"],),
        ).fetchall()
        if len(invocation_rows) > 1:
            raise DomainError("EXPERIENCE_MULTISTEP_NOT_SUPPORTED")
        inv = invocation_rows[0] if invocation_rows else None
        if inv is not None and inv["release_id"] != task["release_id"]:
            raise DomainError("EXPERIENCE_RELEASE_MISMATCH")
        evidence = cls._evidences(c, task, inv)
        commits = c.execute(
            "SELECT * FROM commit_records WHERE task_id=? ORDER BY new_state_version",
            (task["task_id"],),
        ).fetchall()
        for commit in commits:
            if not any(e["evidence_id"] == commit["evidence_id"]
                       and e["verdict_id"] == commit["verdict_id"] for e in evidence):
                raise DomainError("EXPERIENCE_COMMIT_SOURCE_INVALID")
        if status is Status.SUCCEEDED:
            goals = [r for r in commits if r["scope"] == "GOAL"]
            if len(goals) != 1 or not any(r["scope"] == "STEP" for r in commits):
                raise DomainError("EXPERIENCE_UNVERIFIED_SUCCESS")
            # Reuse R04's trusted provenance chain, not provider-supplied success.
            SqliteMemory._validated_goal_source(
                c, cast(str, goals[0]["commit_id"]),
                cast(str, task["task_id"]), cast(str, task["release_id"]),
            )
            trust = ExperienceTrust.VERIFIED
        elif status is Status.HUMAN_REVIEW:
            trust = ExperienceTrust.UNCERTAIN
        else:
            trust = ExperienceTrust.UNVERIFIED
        action = None if inv is None else {
            "invocation_id": inv["invocation_id"], "step_id": inv["step_id"],
            "tool_id": inv["tool_id"], "arguments_hash": inv["arguments_hash"],
            "resource": inv["resource"], "execution_status": inv["status"],
            "provider_status": inv["provider_status"],
        }
        terminal_event = events[-1]
        return {
            "schema_version": 1, "task_id": task["task_id"],
            "release_id": task["release_id"], "release_hash": task["release_hash"],
            "terminal_status": status.value, "trust_level": trust.value,
            "task_goal_hash": content_hash(cls._json_object(
                task["goal_json"], "EXPERIENCE_CONDITION_INVALID")),
            "action": action,
            "observation_action_feedback": evidence,
            "verification": {"goal_commit_id": next(
                (r["commit_id"] for r in commits if r["scope"] == "GOAL"), None),
                "commit_ids": [r["commit_id"] for r in commits]},
            "terminal_event": {"kind": terminal_event["kind"],
                               "reason": terminal_event["payload"].get("reason")
                               or terminal_event["payload"].get("code")
                               or terminal_event["payload"].get("stop_ack")},
            "event_count": len(events), "event_head_hash": task["last_event_hash"],
            "state_version": task["state_version"],
            "limitations": ["FAKE_ADAPTER_ONLY", "ONE_STEP_ONLY",
                            "ACTION_ARGUMENTS_HASH_ONLY", "NO_RAW_SENSOR_ASSETS"],
        }

    def collect_terminal(self, max_tasks: int = 100) -> int:
        """Atomic, replay-safe projection of finished task experiences.

        Corrupt source in a batch aborts the whole batch. A terminal task with
        no invocation is represented with action=None, never invented evidence.
        """
        if type(max_tasks) is not int or not 1 <= max_tasks <= 1000:
            raise DomainError("INVALID_EXPERIENCE_BATCH")
        count = 0
        with self.store._write() as c:
            tasks = c.execute(
                "SELECT * FROM tasks WHERE status IN ('SUCCEEDED','FAILED','HUMAN_REVIEW') "
                "AND NOT EXISTS (SELECT 1 FROM experience_records AS x "
                "WHERE x.task_id=tasks.task_id) ORDER BY created_at,task_id LIMIT ?",
                (max_tasks,),
            ).fetchall()
            for task in tasks:
                episode = self._project(c, task)
                c.execute(
                    "INSERT INTO experience_records VALUES (?,?,?,?,?,?,?,?,?)",
                    (uid(), task["task_id"], task["release_id"], task["status"],
                     episode["trust_level"], canonical_bytes(episode).decode("utf-8"),
                     content_hash(episode), task["last_event_hash"], utc_now()),
                )
                count += 1
        return count

    @staticmethod
    def _record(row: sqlite3.Row) -> ExperienceRecord:
        try:
            content = ExperienceCollector._json_object(
                cast(str, row["content_json"]), "EXPERIENCE_RECORD_TAMPERED",
            )
            if (content_hash(content) != row["content_hash"]
                or content["task_id"] != row["task_id"]
                or content["release_id"] != row["release_id"]
                or content["terminal_status"] != row["terminal_status"]
                or content["trust_level"] != row["trust_level"]
                or content["event_head_hash"] != row["event_head_hash"]):
                raise DomainError("EXPERIENCE_RECORD_TAMPERED")
            return ExperienceRecord(
                cast(str, row["experience_id"]), cast(str, row["task_id"]),
                cast(str, row["release_id"]), Status(row["terminal_status"]),
                ExperienceTrust(row["trust_level"]), content,
                cast(str, row["content_hash"]), cast(str, row["event_head_hash"]),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise DomainError("EXPERIENCE_RECORD_TAMPERED") from exc

    def get(self, task_id: str, *, release_id: str) -> ExperienceRecord | None:
        with closing(self.store._connect()) as c:
            row = c.execute(
                "SELECT * FROM experience_records WHERE task_id=? AND release_id=?",
                (task_id, release_id),
            ).fetchone()
            return None if row is None else self._record(row)

    def search(self, *, release_id: str, trust_level: ExperienceTrust | None = None,
               limit: int = 100) -> list[ExperienceRecord]:
        """Require release scope; no cross-release wildcard and no promotion."""
        if not release_id or type(limit) is not int or not 1 <= limit <= 1000:
            raise DomainError("INVALID_EXPERIENCE_QUERY")
        if trust_level is not None and not isinstance(trust_level, ExperienceTrust):
            raise DomainError("INVALID_EXPERIENCE_TRUST")
        with closing(self.store._connect()) as c:
            if trust_level is None:
                rows = c.execute(
                    "SELECT * FROM experience_records WHERE release_id=? "
                    "ORDER BY projected_at,task_id LIMIT ?", (release_id, limit),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM experience_records WHERE release_id=? AND trust_level=? "
                    "ORDER BY projected_at,task_id LIMIT ?",
                    (release_id, trust_level.value, limit),
                ).fetchall()
            return [self._record(row) for row in rows]
