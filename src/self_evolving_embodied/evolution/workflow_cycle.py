"""Assessment-quality-aware, offline projection from the Single-Agent workflow log.

This is NOT model training and NOT an online policy updater. Historical workflow
facts become immutable, source-linked summaries; deterministic diagnostic
symptoms become *non-executable* review candidates for independent experiments.

Raw sensor payloads never enter this module. An assessor's SUPPORTED outcome is
not environment ground truth. Only trusted application code can run this batch
job; it has no access to the effectful tool dispatcher or robot controllers.
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
from ..runtime.workflow_store import WorkflowStore, _digest
from ..storage.sqlite.database import SqliteStore


class WorkflowDiagnosisKind(StrEnum):
    NO_RECORDED_ISSUE = "NO_RECORDED_ISSUE"
    SUPERSEDED_PLAN = "SUPERSEDED_PLAN"
    EXECUTION_UNCERTAIN = "EXECUTION_UNCERTAIN"
    OBSERVATION_GAP = "OBSERVATION_GAP"
    ASSESSMENT_INCONCLUSIVE = "ASSESSMENT_INCONCLUSIVE"
    GOAL_NOT_SUPPORTED = "GOAL_NOT_SUPPORTED"
    EXECUTION_BLOCKED = "EXECUTION_BLOCKED"
    OUTCOME_UNASSESSED = "OUTCOME_UNASSESSED"


class EvolutionPolicyTarget(StrEnum):
    OBSERVATION_ACQUISITION = "OBSERVATION_ACQUISITION"
    ASSESSMENT_ROUTING = "ASSESSMENT_ROUTING"
    PLANNING_STRATEGY = "PLANNING_STRATEGY"
    TOOL_SELECTION = "TOOL_SELECTION"


@dataclass(frozen=True)
class WorkflowEvolutionRecord:
    run_id: str
    content: dict[str, Any]
    content_hash: str
    source_head_hash: str


@dataclass(frozen=True)
class WorkflowDiagnosisRecord:
    run_id: str
    kind: WorkflowDiagnosisKind
    content: dict[str, Any]
    content_hash: str


@dataclass(frozen=True)
class WorkflowProposalRecord:
    proposal_id: str
    run_id: str
    target: EvolutionPolicyTarget
    content: dict[str, Any]
    content_hash: str


# Safe, deliberately non-executable proposals. These are *hypotheses*, never
# assertions about the physical cause or instructions to change a live device.
_REVIEW_TEMPLATES: dict[WorkflowDiagnosisKind, tuple[EvolutionPolicyTarget, str, str]] = {
    WorkflowDiagnosisKind.OBSERVATION_GAP: (
        EvolutionPolicyTarget.OBSERVATION_ACQUISITION,
        "OBSERVATION_READINESS_REVIEW_V1",
        "Compare pre-action sensor freshness checks and bounded additional observation "
        "requests against the unchanged baseline; do not infer missing sensor data."),
    WorkflowDiagnosisKind.ASSESSMENT_INCONCLUSIVE: (
        EvolutionPolicyTarget.ASSESSMENT_ROUTING,
        "ASSESSOR_SELECTION_REVIEW_V1",
        "Compare candidate assessor selection under partial observability, preserving "
        "UNKNOWN when available evidence is insufficient."),
    WorkflowDiagnosisKind.GOAL_NOT_SUPPORTED: (
        EvolutionPolicyTarget.PLANNING_STRATEGY,
        "GOAL_DECOMPOSITION_REVIEW_V1",
        "Review planning and subtask decomposition against the original unchanged "
        "goal; an assessor disagreement does not establish the physical cause."),
    WorkflowDiagnosisKind.EXECUTION_BLOCKED: (
        EvolutionPolicyTarget.TOOL_SELECTION,
        "TOOL_READINESS_REVIEW_V1",
        "Compare available registered capabilities and pre-dispatch checks without "
        "widening any grants or relaxing physical-action quarantine."),
    WorkflowDiagnosisKind.OUTCOME_UNASSESSED: (
        EvolutionPolicyTarget.ASSESSMENT_ROUTING,
        "ASSESSMENT_COVERAGE_REVIEW_V1",
        "Review which subtasks need independent outcome assessment; do not treat "
        "workflow completion or provider reports as semantic task success."),
}


class WorkflowEvolutionCycle:
    """Idempotent offline pipeline for *new* LLM Agent workflow records.

    The historical single-step ``ExperienceCollector`` and its candidate,
    frozen-suite and release gates remain separate and unchanged. New workflow
    proposals cannot enter their fake-only evaluator/release path by accident.

    Writes are immutable and protected by the existing SqliteStore lease checks.
    Caller should run this when no worker owns the database, or under a trusted
    ``worker_session``. No model/provider, ROS, sensor or robot API is invoked.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _json(value: str, code: str) -> dict[str, Any]:
        try:
            obj = json.loads(value)
        except (ValueError, TypeError) as exc:
            raise DomainError(code) from exc
        if not isinstance(obj, dict):
            raise DomainError(code)
        return cast(dict[str, Any], obj)

    @classmethod
    def _project(cls, c: sqlite3.Connection, workflow: sqlite3.Row) -> dict[str, Any]:
        """Read only, verify the *entire* workflow hash chain before projection."""
        WorkflowStore._check_chain(c, workflow)
        if workflow["status"] not in {"SUCCEEDED", "BLOCKED", "HUMAN_REVIEW"}:
            raise DomainError("EVOLUTION_WORKFLOW_NOT_TERMINAL")
        try:
            plan = cls._json(workflow["plan_json"], "EVOLUTION_PLAN_INVALID")
            if _digest(plan) != workflow["plan_hash"]:
                raise DomainError("EVOLUTION_PLAN_INVALID")
            declared = plan["subtasks"]
            if not isinstance(declared, list) or not declared:
                raise DomainError("EVOLUTION_PLAN_INVALID")
        except (KeyError, TypeError) as exc:
            raise DomainError("EVOLUTION_PLAN_INVALID") from exc
        rows = c.execute(
            "SELECT subtask_id,ordinal,tool_id,status,result_json,failure_code "
            "FROM agent_subtasks WHERE run_id=? ORDER BY ordinal",
            (workflow["run_id"],),
        ).fetchall()
        if len(rows) != len(declared):
            raise DomainError("EVOLUTION_SUBTASKS_INVALID")
        turns = c.execute(
            "SELECT payload_json FROM agent_workflow_events "
            "WHERE run_id=? AND kind='AGENT_TURN_RECORDED' ORDER BY seq",
            (workflow["run_id"],),
        ).fetchall()
        latest: dict[str, dict[str, Any]] = {}
        assessment_attempts: dict[str, int] = {}
        for row in turns:
            turn = cls._json(row["payload_json"], "EVOLUTION_TURN_INVALID")
            key = turn.get("subtask_id")
            if not isinstance(key, str) or not any(s["subtask_id"] == key for s in rows):
                raise DomainError("EVOLUTION_TURN_INVALID")
            assessment_attempts[key] = assessment_attempts.get(key, 0) + 1
            latest[key] = turn

        steps: list[dict[str, Any]] = []
        assessed = supported = negative = unknown = referenced = 0
        for ordinal, (step, original) in enumerate(zip(rows, declared)):
            if (not isinstance(original, dict) or step["ordinal"] != ordinal
                or step["subtask_id"] != original.get("id")
                or step["tool_id"] != original.get("tool_id")):
                raise DomainError("EVOLUTION_SUBTASKS_INVALID")
            result_hash = None
            if step["result_json"] is not None:
                result_hash = _digest(cls._json(step["result_json"], "EVOLUTION_TOOL_RESULT_INVALID"))
            turn = latest.get(step["subtask_id"])
            outcome = "NOT_ASSESSED"
            assessor_id = None
            obs_count = 0
            action = None
            if turn is not None:
                action = turn.get("action")
                raw = turn.get("assessment")
                if raw is not None:
                    if not isinstance(raw, dict):
                        raise DomainError("EVOLUTION_TURN_INVALID")
                    assessor_id = raw.get("assessor_id")
                    outcome = raw.get("outcome")
                    refs = raw.get("observation_refs", [])
                    if (not isinstance(assessor_id, str)
                        or outcome not in {"SUPPORTED", "NOT_SUPPORTED", "UNKNOWN"}
                        or not isinstance(refs, list) or len(refs) > 64
                        or turn.get("outcome") != outcome):
                        raise DomainError("EVOLUTION_TURN_INVALID")
                    obs_count = len(refs)
                    assessed += 1
                    supported += outcome == "SUPPORTED"
                    negative += outcome == "NOT_SUPPORTED"
                    unknown += outcome == "UNKNOWN"
                    referenced += bool(refs)
                elif turn.get("outcome") != "NOT_ASSESSED":
                    raise DomainError("EVOLUTION_TURN_INVALID")
            # Raw tool returns, actions, frames and sensor contents never enter
            # these rows. An observation reference is not durable sensor evidence.
            steps.append({
                "subtask_id": step["subtask_id"], "tool_id": step["tool_id"],
                "execution_status": step["status"],
                "failure_code": step["failure_code"],
                "tool_result_hash": result_hash,
                "assessment_outcome": outcome, "assessor_id": assessor_id,
                "observation_reference_count": obs_count,
                "assessment_attempts": assessment_attempts.get(step["subtask_id"], 0),
                "agent_action": action,
            })
        if assessed == len(steps) and supported == assessed and workflow["status"] == "SUCCEEDED":
            semantic_outcome = "ASSESSOR_SUPPORTED_NOT_GROUND_TRUTH"
        elif negative:
            semantic_outcome = "ASSESSOR_NOT_SUPPORTED"
        else:
            semantic_outcome = "UNKNOWN"
        quality = ("NO_ASSESSMENT" if assessed == 0 else
                   "ALL_ASSESSMENTS_REFERENCE_EPHEMERAL_OBSERVATIONS" if referenced == assessed else
                   "PARTIAL_OR_REPORT_ONLY")
        return {
            "schema_version": 1,
            "source_type": "SINGLE_AGENT_WORKFLOW",
            "run_id": workflow["run_id"], "plan_hash": workflow["plan_hash"],
            "tool_catalog_hash": workflow["catalog_hash"],
            "source_event_head_hash": workflow["last_event_hash"],
            "source_event_count": workflow["event_seq"],
            "workflow_status": workflow["status"],
            "workflow_failure_code": workflow["failure_code"],
            "semantic_outcome": semantic_outcome,
            "assessment_provenance_quality": quality,
            "assessment_counts": {"assessed": assessed, "supported": supported,
                                  "not_supported": negative, "unknown": unknown,
                                  "observation_referenced": referenced},
            "steps": steps,
            "ground_truth_available": False,
            "raw_observation_persisted": False,
            "limitations": ["ASSESSOR_OUTPUT_IS_INFERENCE", "EPHEMERAL_OBSERVATION_REFS",
                            "NOT_INDEPENDENT_HELDOUT_EVALUATION"],
        }

    @classmethod
    def _experience(cls, c: sqlite3.Connection, run_id: str) -> WorkflowEvolutionRecord:
        row = c.execute(
            "SELECT * FROM agent_evolution_experiences WHERE run_id=?", (run_id,),
        ).fetchone()
        if row is None:
            raise DomainError("WORKFLOW_EXPERIENCE_NOT_FOUND")
        body = cls._json(row["content_json"], "WORKFLOW_EXPERIENCE_TAMPERED")
        src = WorkflowStore._workflow(c, run_id)
        expected = cls._project(c, src)
        if (body != expected or content_hash(body) != row["content_hash"]
            or body["source_event_head_hash"] != row["source_head_hash"]
            or body["run_id"] != run_id):
            raise DomainError("WORKFLOW_EXPERIENCE_SOURCE_CHANGED")
        return WorkflowEvolutionRecord(run_id, body, row["content_hash"], row["source_head_hash"])

    @staticmethod
    def _diagnose(body: dict[str, Any]) -> dict[str, Any]:
        counts = body["assessment_counts"]
        status = body["workflow_status"]
        failure = body["workflow_failure_code"]
        if failure == "AGENT_REPLANNED":
            kind = WorkflowDiagnosisKind.SUPERSEDED_PLAN
        elif status == "HUMAN_REVIEW":
            kind = WorkflowDiagnosisKind.EXECUTION_UNCERTAIN
        elif counts["unknown"] > 0:
            kind = (WorkflowDiagnosisKind.OBSERVATION_GAP
                    if counts["observation_referenced"] < counts["assessed"]
                    else WorkflowDiagnosisKind.ASSESSMENT_INCONCLUSIVE)
        elif counts["not_supported"] > 0:
            kind = WorkflowDiagnosisKind.GOAL_NOT_SUPPORTED
        elif status == "BLOCKED":
            kind = WorkflowDiagnosisKind.EXECUTION_BLOCKED
        elif counts["assessed"] < len(body["steps"]):
            kind = WorkflowDiagnosisKind.OUTCOME_UNASSESSED
        else:
            kind = WorkflowDiagnosisKind.NO_RECORDED_ISSUE
        return {
            "schema_version": 1, "run_id": body["run_id"],
            "source_experience_hash": content_hash(body),
            "source_event_head_hash": body["source_event_head_hash"],
            "kind": kind.value,
            "classification_basis": "DETERMINISTIC_RECORDED_SYMPTOM",
            "root_cause": "NOT_ESTABLISHED",
            "assessment_is_ground_truth": False,
            "suggestion_authorized": False,
            "automatic_policy_update_authorized": False,
            "notes": "A workflow status or model assessment is not physical task ground truth.",
        }

    @classmethod
    def _diagnosis(cls, c: sqlite3.Connection, run_id: str) -> WorkflowDiagnosisRecord:
        row = c.execute(
            "SELECT * FROM agent_evolution_diagnoses WHERE run_id=?", (run_id,),
        ).fetchone()
        if row is None:
            raise DomainError("WORKFLOW_DIAGNOSIS_NOT_FOUND")
        experience = cls._experience(c, run_id)
        body = cls._json(row["content_json"], "WORKFLOW_DIAGNOSIS_TAMPERED")
        if (body != cls._diagnose(experience.content)
            or content_hash(body) != row["content_hash"]
            or row["source_experience_hash"] != experience.content_hash
            or row["kind"] != body["kind"]):
            raise DomainError("WORKFLOW_DIAGNOSIS_TAMPERED")
        try:
            kind = WorkflowDiagnosisKind(row["kind"])
        except (ValueError, TypeError) as exc:
            raise DomainError("WORKFLOW_DIAGNOSIS_TAMPERED") from exc
        return WorkflowDiagnosisRecord(run_id, kind, body, row["content_hash"])

    @classmethod
    def _proposal_payload(cls, diagnosis: WorkflowDiagnosisRecord,
                          experience: WorkflowEvolutionRecord) -> dict[str, Any] | None:
        if diagnosis.kind in {WorkflowDiagnosisKind.EXECUTION_UNCERTAIN,
                              WorkflowDiagnosisKind.SUPERSEDED_PLAN,
                              WorkflowDiagnosisKind.NO_RECORDED_ISSUE}:
            return None
        if (diagnosis.content["root_cause"] != "NOT_ESTABLISHED"
            or diagnosis.content["automatic_policy_update_authorized"] is not False):
            raise DomainError("EVOLUTION_DIAGNOSIS_INVALID")
        template = _REVIEW_TEMPLATES.get(diagnosis.kind)
        if template is None:
            return None
        target, template_id, suggestion = template
        return {
            "schema_version": 1, "kind": "NON_EXECUTABLE_RESEARCH_PROPOSAL",
            "status": "PROPOSED_NOT_EVALUATED",
            "run_id": diagnosis.run_id,
            "source_diagnosis_hash": diagnosis.content_hash,
            "source_experience_hash": experience.content_hash,
            "baseline_plan_hash": experience.content["plan_hash"],
            "baseline_catalog_hash": experience.content["tool_catalog_hash"],
            "diagnosis_kind": diagnosis.kind.value,
            "root_cause": "NOT_ESTABLISHED", "target": target.value,
            "template_id": template_id, "hypothesis": suggestion,
            "requires_independent_heldout_evaluation": True,
            "requires_human_approval": True,
            "apply_authorized": False, "publish_authorized": False,
            "modifies_live_runtime": False,
            "disallowed_targets": ["SAFETY_GRANTS", "PHYSICAL_CONTROLLER", "DEVICE_FENCING",
                                   "SUCCESS_LABELS", "RAW_SENSOR_QUEUE"],
        }

    @classmethod
    def _proposal(cls, c: sqlite3.Connection, run_id: str) -> WorkflowProposalRecord:
        row = c.execute("SELECT * FROM agent_evolution_proposals WHERE run_id=?",
                        (run_id,)).fetchone()
        if row is None:
            raise DomainError("WORKFLOW_PROPOSAL_NOT_FOUND")
        diagnosis = cls._diagnosis(c, run_id)
        experience = cls._experience(c, run_id)
        body = cls._json(row["content_json"], "WORKFLOW_PROPOSAL_TAMPERED")
        expected = cls._proposal_payload(diagnosis, experience)
        if (expected is None or body != expected
            or content_hash(body) != row["content_hash"]
            or row["source_diagnosis_hash"] != diagnosis.content_hash
            or row["target"] != body["target"]):
            raise DomainError("WORKFLOW_PROPOSAL_TAMPERED")
        decision = c.execute("SELECT decision FROM agent_evolution_decisions WHERE run_id=?",
                             (run_id,)).fetchone()
        if decision is None or decision["decision"] != "PROPOSED":
            raise DomainError("WORKFLOW_PROPOSAL_DECISION_INVALID")
        try:
            target = EvolutionPolicyTarget(row["target"])
        except (ValueError, TypeError) as exc:
            raise DomainError("WORKFLOW_PROPOSAL_TAMPERED") from exc
        return WorkflowProposalRecord(row["proposal_id"], run_id,
                                      target, body, row["content_hash"])

    @staticmethod
    def _batch_size(limit: int) -> None:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise DomainError("INVALID_EVOLUTION_BATCH")

    def collect_terminal(self, limit: int = 100) -> int:
        """Project terminal LLM workflows, not the legacy single-step Fake tasks."""
        self._batch_size(limit)
        created = 0
        with self.store._write() as c:
            rows = c.execute(
                "SELECT * FROM agent_workflows WHERE status IN "
                "('SUCCEEDED','BLOCKED','HUMAN_REVIEW') AND NOT EXISTS "
                "(SELECT 1 FROM agent_evolution_experiences AS x "
                "WHERE x.run_id=agent_workflows.run_id) "
                "ORDER BY created_at,run_id LIMIT ?", (limit,),
            ).fetchall()
            for row in rows:
                body = self._project(c, row)
                c.execute(
                    "INSERT INTO agent_evolution_experiences "
                    "(run_id,source_head_hash,content_json,content_hash,created_at) "
                    "VALUES (?,?,?,?,?)",
                    (row["run_id"], row["last_event_hash"],
                     canonical_bytes(body).decode("utf-8"), content_hash(body), utc_now()),
                )
                created += 1
        return created

    def diagnose_pending(self, limit: int = 100) -> int:
        self._batch_size(limit)
        created = 0
        with self.store._write() as c:
            rows = c.execute(
                "SELECT run_id FROM agent_evolution_experiences AS x WHERE NOT EXISTS "
                "(SELECT 1 FROM agent_evolution_diagnoses AS d WHERE d.run_id=x.run_id) "
                "ORDER BY created_at,run_id LIMIT ?", (limit,),
            ).fetchall()
            for row in rows:
                experience = self._experience(c, row["run_id"])
                body = self._diagnose(experience.content)
                c.execute(
                    "INSERT INTO agent_evolution_diagnoses "
                    "(run_id,source_experience_hash,kind,content_json,content_hash,created_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (row["run_id"], experience.content_hash, body["kind"],
                     canonical_bytes(body).decode("utf-8"), content_hash(body), utc_now()),
                )
                created += 1
        return created

    def propose_pending(self, limit: int = 100) -> int:
        """Only fixed, non-executable proposals. Unsafe episodes are skipped."""
        self._batch_size(limit)
        created = 0
        with self.store._write() as c:
            rows = c.execute(
                "SELECT run_id FROM agent_evolution_diagnoses AS d WHERE NOT EXISTS "
                "(SELECT 1 FROM agent_evolution_decisions AS p WHERE p.run_id=d.run_id) "
                "ORDER BY created_at,run_id LIMIT ?", (limit,),
            ).fetchall()
            for row in rows:
                diagnosis = self._diagnosis(c, row["run_id"])
                experience = self._experience(c, row["run_id"])
                payload = self._proposal_payload(diagnosis, experience)
                decision = "SKIPPED_UNSAFE_OR_UNSUPPORTED"
                if payload is not None:
                    proposal_id = uid()
                    c.execute(
                        "INSERT INTO agent_evolution_proposals "
                        "(proposal_id,run_id,source_diagnosis_hash,target,content_json,"
                        "content_hash,created_at) VALUES (?,?,?,?,?,?,?)",
                        (proposal_id, row["run_id"], diagnosis.content_hash,
                         payload["target"], canonical_bytes(payload).decode("utf-8"),
                         content_hash(payload), utc_now()),
                    )
                    decision = "PROPOSED"
                    created += 1
                c.execute(
                    "INSERT INTO agent_evolution_decisions "
                    "(run_id,source_diagnosis_hash,decision,created_at) VALUES (?,?,?,?)",
                    (row["run_id"], diagnosis.content_hash, decision, utc_now()),
                )
        return created

    def run_cycle(self, limit: int = 100) -> dict[str, int]:
        """Single offline batch. No evaluation/promotion happens here."""
        return {"experiences": self.collect_terminal(limit),
                "diagnoses": self.diagnose_pending(limit),
                "proposals": self.propose_pending(limit)}

    def get_experience(self, run_id: str) -> WorkflowEvolutionRecord | None:
        with closing(self.store._connect()) as c:
            if c.execute("SELECT 1 FROM agent_evolution_experiences WHERE run_id=?",
                         (run_id,)).fetchone() is None:
                return None
            return self._experience(c, run_id)

    def get_diagnosis(self, run_id: str) -> WorkflowDiagnosisRecord | None:
        with closing(self.store._connect()) as c:
            if c.execute("SELECT 1 FROM agent_evolution_diagnoses WHERE run_id=?",
                         (run_id,)).fetchone() is None:
                return None
            return self._diagnosis(c, run_id)

    def get_proposal(self, run_id: str) -> WorkflowProposalRecord | None:
        with closing(self.store._connect()) as c:
            if c.execute("SELECT 1 FROM agent_evolution_proposals WHERE run_id=?",
                         (run_id,)).fetchone() is None:
                return None
            return self._proposal(c, run_id)
