"""Independent held-out policy studies for Single-Agent workflow evolution (v16).

This is an OFFLINE RESEARCH API; never wired into robot controllers, live LLM
routing, model training, or the legacy R0-FAKE release. StudyRunner is a trusted,
externally supplied benchmark integration, not an LLM-provided tool. A claimed
independent oracle is *provenance metadata*, not cryptographic proof of truth.
No raw sensor payloads are recorded in SQLite.
"""
from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError
from ..core.ids import uid, utc_now
from ..evolution.workflow_cycle import EvolutionPolicyTarget, WorkflowEvolutionCycle
from ..storage.sqlite.database import SqliteStore

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}\Z")
_POLICY_FIELDS: dict[str, dict[str, tuple[type, int, int] | type]] = {
    "OBSERVATION_ACQUISITION": {"max_observation_age_ms": (int, 1, 10000), "retry_observation_budget": (int, 0, 4)},
    "ASSESSMENT_ROUTING": {"prefer_independent_vision": bool, "max_uncertain_assessments": (int, 0, 4)},
    "PLANNING_STRATEGY": {"max_replans": (int, 0, 5), "require_pre_observation": bool},
    "TOOL_SELECTION": {"prefer_read_only_first": bool, "max_tool_calls": (int, 1, 30)},
}


def _ident(value: Any, code: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise DomainError(code)
    return value


def _load_object(text: Any, digest: Any, code: str) -> dict[str, Any]:
    import json
    try:
        value = json.loads(text)
        if (not isinstance(value, dict) or
            text != canonical_bytes(value).decode("utf-8") or
            content_hash(value) != digest):
            raise DomainError(code)
    except (ValueError, TypeError, UnicodeError) as exc:
        raise DomainError(code) from exc
    return value


@dataclass(frozen=True)
class PolicyProfile:
    """Strictly declarative, research-only knobs; NO authorization/safety fields."""
    target: str
    settings: dict[str, Any]

    def __post_init__(self) -> None:
        try:
            EvolutionPolicyTarget(self.target)
        except (ValueError, TypeError) as exc:
            raise DomainError("POLICY_TARGET_INVALID") from exc
        schema = _POLICY_FIELDS[self.target]
        if type(self.settings) is not dict or set(self.settings) != set(schema):
            raise DomainError("POLICY_FIELDS_NOT_ALLOWLISTED")
        for k, typ in schema.items():
            v = self.settings[k]
            if isinstance(typ, tuple):
                dtype, minimum, maximum = typ
                if type(v) is not dtype or not minimum <= v <= maximum:
                    raise DomainError("POLICY_VALUE_INVALID")
            elif type(v) is not typ:
                raise DomainError("POLICY_VALUE_INVALID")
        object.__setattr__(self, "settings", MappingProxyType(dict(self.settings)))

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "scope": "OFFLINE_RESEARCH_ONLY",
                "target": self.target, "settings": dict(self.settings)}

    @property
    def digest(self) -> str:
        return content_hash(self.to_dict())


@dataclass(frozen=True)
class HeldoutCase:
    case_id: str
    episode_id: str
    benchmark_ref: str
    oracle_ref: str

    def __post_init__(self) -> None:
        for val in (self.case_id, self.episode_id, self.benchmark_ref, self.oracle_ref):
            _ident(val, "HELDOUT_CASE_INVALID")

    def to_dict(self) -> dict[str, str]:
        return {"case_id": self.case_id, "episode_id": self.episode_id,
                "benchmark_ref": self.benchmark_ref, "oracle_ref": self.oracle_ref}


@dataclass(frozen=True)
class HeldoutSuite:
    suite_id: str
    benchmark_version: str
    split_id: str
    oracle_authority_id: str
    cases: tuple[HeldoutCase, ...]

    def __post_init__(self) -> None:
        for v in (self.suite_id, self.benchmark_version, self.split_id, self.oracle_authority_id):
            _ident(v, "HELDOUT_SUITE_INVALID")
        if type(self.cases) is not tuple or not 8 <= len(self.cases) <= 1000 or any(not isinstance(c, HeldoutCase) for c in self.cases):
            raise DomainError("HELDOUT_SUITE_TOO_SMALL_OR_INVALID")
        if (len({c.case_id for c in self.cases}) != len(self.cases) or
            len({c.episode_id for c in self.cases}) != len(self.cases)):
            raise DomainError("HELDOUT_SUITE_DUPLICATE_CASE")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "suite_kind": "INDEPENDENT_HELDOUT_EXTERNAL_DATA",
                "suite_id": self.suite_id, "benchmark_version": self.benchmark_version,
                "split_id": self.split_id, "oracle_authority_id": self.oracle_authority_id,
                "cases": [case.to_dict() for case in self.cases]}

    @property
    def digest(self) -> str:
        return content_hash(self.to_dict())


@dataclass(frozen=True)
class TrialEvidence:
    """External independent oracle is required, separate from model's assessment.

    applied_policy_hash is emitted by the trusted benchmark runner. No images,
    point clouds, prompts or arbitrary tool responses should be included here.
    """
    case_id: str
    applied_policy_hash: str
    oracle_ref: str
    oracle_success: bool
    agent_claimed_success: bool
    unsafe_retry_count: int
    latency_ms: int
    oracle_provenance: str = "INDEPENDENT_HELDOUT_ORACLE"

    def __post_init__(self) -> None:
        for v in (self.case_id, self.oracle_ref):
            _ident(v, "TRIAL_EVIDENCE_INVALID")
        if (type(self.applied_policy_hash) is not str or len(self.applied_policy_hash) != 64 or
            not all(x in "0123456789abcdef" for x in self.applied_policy_hash) or
            type(self.oracle_success) is not bool or type(self.agent_claimed_success) is not bool or
            type(self.unsafe_retry_count) is not int or not 0 <= self.unsafe_retry_count <= 1000 or
            type(self.latency_ms) is not int or not 0 <= self.latency_ms <= 60_000_000 or
            self.oracle_provenance != "INDEPENDENT_HELDOUT_ORACLE"):
            raise DomainError("TRIAL_EVIDENCE_INVALID")

    def to_dict(self) -> dict[str, Any]:
        return {"case_id": self.case_id, "applied_policy_hash": self.applied_policy_hash,
                "oracle_ref": self.oracle_ref, "oracle_success": self.oracle_success,
                "agent_claimed_success": self.agent_claimed_success,
                "unsafe_retry_count": self.unsafe_retry_count, "latency_ms": self.latency_ms,
                "oracle_provenance": self.oracle_provenance}


class StudyRunner(Protocol):
    """Injected trusted OFFLINE evaluator; it must genuinely apply the variant.

    The framework cannot verify external benchmark isolation by code alone.
    Runner must not call live robot/controllers or reuse Agent self-assessments
    as an oracle. Recommended: disconnected simulator/test dataset + independent
    evaluator, with a documented heldout split and pinned model/tool versions.
    """
    evaluator_id: str
    operator_id: str

    def evaluate_case(self, case: HeldoutCase, policy: PolicyProfile) -> TrialEvidence: ...


@dataclass(frozen=True)
class PolicyStudyRecord:
    study_id: str
    proposal_id: str
    report: dict[str, Any]
    report_hash: str


class IndependentPolicyEvaluator:
    """Paired independent off-line comparison, append-only record, strict gate."""

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _read_suite(c: sqlite3.Connection, suite_id: str) -> HeldoutSuite:
        row = c.execute("SELECT * FROM agent_heldout_suites WHERE suite_id=?", (suite_id,)).fetchone()
        if row is None:
            raise DomainError("HELDOUT_SUITE_NOT_FOUND")
        data = _load_object(row["definition_json"], row["definition_hash"], "HELDOUT_SUITE_TAMPERED")
        try:
            suite = HeldoutSuite(data["suite_id"], data["benchmark_version"], data["split_id"],
                                 data["oracle_authority_id"],
                                 tuple(HeldoutCase(**case) for case in data["cases"]))
        except (KeyError, TypeError) as exc:
            raise DomainError("HELDOUT_SUITE_TAMPERED") from exc
        if suite.to_dict() != data or suite.suite_id != suite_id:
            raise DomainError("HELDOUT_SUITE_TAMPERED")
        return suite

    def freeze_suite(self, suite: HeldoutSuite) -> str:
        """Pin external heldout identities. Does NOT store sensor observations."""
        if not isinstance(suite, HeldoutSuite):
            raise DomainError("HELDOUT_SUITE_INVALID")
        with self.store._write() as c:
            # Exclude the entire known training/experience population from heldout.
            for case in suite.cases:
                if (c.execute("SELECT 1 FROM agent_workflows WHERE run_id=?", (case.episode_id,)).fetchone()
                    or c.execute("SELECT 1 FROM agent_evolution_experiences WHERE run_id=?", (case.episode_id,)).fetchone()):
                    raise DomainError("HELDOUT_OVERLAPS_WORKFLOW_HISTORY")
            existing = c.execute("SELECT 1 FROM agent_heldout_suites WHERE suite_id=?", (suite.suite_id,)).fetchone()
            if existing:
                saved = self._read_suite(c, suite.suite_id)
                if saved.to_dict() != suite.to_dict():
                    raise DomainError("HELDOUT_SUITE_ID_CONFLICT")
                return saved.digest
            c.execute("INSERT INTO agent_heldout_suites VALUES (?,?,?,?)",
                      (suite.suite_id, canonical_bytes(suite.to_dict()).decode("utf-8"),
                       suite.digest, utc_now()))
        return suite.digest

    @staticmethod
    def _score(rows: list[dict[str, Any]]) -> dict[str, int]:
        return {"oracle_success": sum(x["oracle_success"] for x in rows),
                "false_success_claims": sum(x["agent_claimed_success"] and not x["oracle_success"] for x in rows),
                "unsafe_retry_count": sum(x["unsafe_retry_count"] for x in rows),
                "latency_total_ms": sum(x["latency_ms"] for x in rows)}

    @classmethod
    def _report(cls, *, proposal: Any, suite: HeldoutSuite,
                baseline: PolicyProfile, candidate: PolicyProfile,
                runner: StudyRunner) -> dict[str, Any]:
        base_rows: list[dict[str, Any]] = []
        cand_rows: list[dict[str, Any]] = []
        for case in suite.cases:
            # Offline benchmark runner is trusted application code and must not
            # share an evaluator self-report as its external oracle.
            b = runner.evaluate_case(case, baseline)
            d = runner.evaluate_case(case, candidate)
            if not isinstance(b, TrialEvidence) or not isinstance(d, TrialEvidence):
                raise DomainError("OFFLINE_RUNNER_INVALID_RESULT")
            if (b.case_id != case.case_id or d.case_id != case.case_id
                or b.oracle_ref != case.oracle_ref or d.oracle_ref != case.oracle_ref
                or b.applied_policy_hash != baseline.digest
                or d.applied_policy_hash != candidate.digest
                ):
                raise DomainError("INDEPENDENT_PAIRED_ORACLE_INVALID")
            base_rows.append(b.to_dict())
            cand_rows.append(d.to_dict())
        bscore, cscore = cls._score(base_rows), cls._score(cand_rows)
        passed = (cscore["oracle_success"] > bscore["oracle_success"]
                  and cscore["false_success_claims"] <= bscore["false_success_claims"]
                  and cscore["unsafe_retry_count"] == 0
                  and cscore["latency_total_ms"] * 100 <= bscore["latency_total_ms"] * 120)
        reasons: list[str] = []
        if cscore["oracle_success"] <= bscore["oracle_success"]:
            reasons.append("NO_INDEPENDENT_ORACLE_SUCCESS_IMPROVEMENT")
        if cscore["false_success_claims"] > bscore["false_success_claims"]:
            reasons.append("FALSE_SUCCESS_CLAIM_REGRESSION")
        if cscore["unsafe_retry_count"] != 0:
            reasons.append("UNSAFE_RETRIES_PRESENT")
        if cscore["latency_total_ms"] * 100 > bscore["latency_total_ms"] * 120:
            reasons.append("LATENCY_REGRESSION")
        return {
            "schema_version": 1, "study_kind": "PAIRED_EXTERNAL_OFFLINE_POLICY_COMPARISON",
            "research_only": True, "changes_live_runtime": False,
            "proposal_id": proposal.proposal_id, "proposal_hash": proposal.content_hash,
            "source_run_id": proposal.run_id, "target": proposal.target.value,
            "suite_id": suite.suite_id, "suite_hash": suite.digest,
            "benchmark_version": suite.benchmark_version, "heldout_split_id": suite.split_id,
            "oracle_authority_id": suite.oracle_authority_id,
            "evaluator_id": runner.evaluator_id, "operator_id": runner.operator_id,
            "baseline_policy": baseline.to_dict(), "baseline_hash": baseline.digest,
            "candidate_policy": candidate.to_dict(), "candidate_hash": candidate.digest,
            "baseline_rows": base_rows, "candidate_rows": cand_rows,
            "case_count": len(suite.cases), "baseline_metrics": bscore, "candidate_metrics": cscore,
            "gate": "PASS" if passed else "BLOCKED", "blockers": reasons,
            "oracle_labels_inferred_from_agent_assessment": False,
            "independent_oracle_self_declared_by_trusted_runner": True,
            "promotion_requires_human_review": True,
            "production_release_authorized": False,
        }

    @classmethod
    def _read_study(cls, c: sqlite3.Connection, study_id: str) -> PolicyStudyRecord:
        row = c.execute("SELECT * FROM agent_policy_studies WHERE study_id=?", (study_id,)).fetchone()
        if row is None:
            raise DomainError("OFFLINE_STUDY_NOT_FOUND")
        body = _load_object(row["report_json"], row["report_hash"], "OFFLINE_STUDY_TAMPERED")
        linked = c.execute("SELECT run_id FROM agent_evolution_proposals WHERE proposal_id=?",
                           (row["proposal_id"],)).fetchone()
        if linked is None:
            raise DomainError("OFFLINE_STUDY_SOURCE_MISSING")
        proposal = WorkflowEvolutionCycle._proposal(c, linked["run_id"])
        suite = cls._read_suite(c, row["suite_id"])
        if (proposal.proposal_id != row["proposal_id"] or
            body.get("proposal_hash") != proposal.content_hash or
            body.get("source_run_id") != proposal.run_id or
            body.get("target") != proposal.target.value or
            body.get("suite_hash") != suite.digest or
            body.get("suite_id") != suite.suite_id or
            body.get("baseline_hash") != row["baseline_hash"] or
            body.get("candidate_hash") != row["candidate_hash"] or
            content_hash(body.get("baseline_policy")) != row["baseline_hash"] or
            content_hash(body.get("candidate_policy")) != row["candidate_hash"] or
            body.get("study_kind") != "PAIRED_EXTERNAL_OFFLINE_POLICY_COMPARISON" or
            body.get("production_release_authorized") is not False):
            raise DomainError("OFFLINE_STUDY_SOURCE_CHANGED")
        try:
            baseline_policy = body["baseline_policy"]
            candidate_policy = body["candidate_policy"]
            if (not isinstance(baseline_policy, dict) or not isinstance(candidate_policy, dict)
                or any(set(profile) != {"schema_version", "scope", "target", "settings"}
                       for profile in (baseline_policy, candidate_policy))):
                raise DomainError("OFFLINE_STUDY_POLICY_INVALID")
            base_profile = PolicyProfile(baseline_policy["target"], baseline_policy["settings"])
            cand_profile = PolicyProfile(candidate_policy["target"], candidate_policy["settings"])
            if (base_profile.to_dict() != baseline_policy or cand_profile.to_dict() != candidate_policy
                or base_profile.target != proposal.target.value
                or cand_profile.target != proposal.target.value or
                base_profile.digest == cand_profile.digest):
                raise DomainError("OFFLINE_STUDY_POLICY_INVALID")
        except (KeyError, ValueError, TypeError) as exc:
            raise DomainError("OFFLINE_STUDY_POLICY_INVALID") from exc
        if (body.get("research_only") is not True or body.get("changes_live_runtime") is not False
            or body.get("oracle_authority_id") != suite.oracle_authority_id
            or body.get("heldout_split_id") != suite.split_id
            or body.get("benchmark_version") != suite.benchmark_version
            or body.get("independent_oracle_self_declared_by_trusted_runner") is not True
            or body.get("oracle_labels_inferred_from_agent_assessment") is not False
            or body.get("case_count") != len(suite.cases)
            or not isinstance(body.get("evaluator_id"), str)
            or not isinstance(body.get("operator_id"), str)):
            raise DomainError("OFFLINE_STUDY_METADATA_TAMPERED")
        # Validate report against the persisted paired raw metrics, not only hash.
        b_rows, c_rows = body.get("baseline_rows"), body.get("candidate_rows")
        if (not isinstance(b_rows, list) or not isinstance(c_rows, list)
            or len(b_rows) != len(suite.cases) or len(c_rows) != len(suite.cases)):
            raise DomainError("OFFLINE_STUDY_METRICS_TAMPERED")
        for case, br, cr in zip(suite.cases, b_rows, c_rows):
            try:
                b, d = TrialEvidence(**br), TrialEvidence(**cr)
            except (TypeError, ValueError) as exc:
                raise DomainError("OFFLINE_STUDY_METRICS_TAMPERED") from exc
            if (b.case_id != case.case_id or d.case_id != case.case_id
                or b.oracle_ref != case.oracle_ref or d.oracle_ref != case.oracle_ref
                or b.applied_policy_hash != row["baseline_hash"]
                or d.applied_policy_hash != row["candidate_hash"]
                ):
                raise DomainError("OFFLINE_STUDY_METRICS_TAMPERED")
        bm, cm = cls._score(b_rows), cls._score(c_rows)
        passed = (cm["oracle_success"] > bm["oracle_success"]
                  and cm["false_success_claims"] <= bm["false_success_claims"]
                  and cm["unsafe_retry_count"] == 0
                  and cm["latency_total_ms"] * 100 <= bm["latency_total_ms"] * 120)
        expected_blockers = []
        if cm["oracle_success"] <= bm["oracle_success"]:
            expected_blockers.append("NO_INDEPENDENT_ORACLE_SUCCESS_IMPROVEMENT")
        if cm["false_success_claims"] > bm["false_success_claims"]:
            expected_blockers.append("FALSE_SUCCESS_CLAIM_REGRESSION")
        if cm["unsafe_retry_count"]:
            expected_blockers.append("UNSAFE_RETRIES_PRESENT")
        if cm["latency_total_ms"] * 100 > bm["latency_total_ms"] * 120:
            expected_blockers.append("LATENCY_REGRESSION")
        if (body.get("baseline_metrics") != bm or body.get("candidate_metrics") != cm
            or body.get("gate") != ("PASS" if passed else "BLOCKED")
            or body.get("blockers") != expected_blockers):
            raise DomainError("OFFLINE_STUDY_METRICS_TAMPERED")
        return PolicyStudyRecord(study_id, row["proposal_id"], body, row["report_hash"])

    def evaluate(self, *, proposal_run_id: str, suite_id: str,
                 baseline: PolicyProfile, candidate: PolicyProfile,
                 runner: StudyRunner) -> PolicyStudyRecord:
        if (not isinstance(baseline, PolicyProfile) or not isinstance(candidate, PolicyProfile)
            or baseline.target != candidate.target or baseline.digest == candidate.digest
            or runner is None):
            raise DomainError("POLICY_COMPARISON_INVALID")
        _ident(runner.evaluator_id, "STUDY_EVALUATOR_INVALID")
        _ident(runner.operator_id, "STUDY_OPERATOR_INVALID")
        # Read and validate all frozen inputs before calling external offline code.
        with closing(self.store._connect()) as c:
            proposal = WorkflowEvolutionCycle._proposal(c, proposal_run_id)
            suite = self._read_suite(c, suite_id)
            if proposal.target.value != baseline.target or any(
                case.episode_id == proposal.run_id for case in suite.cases
            ):
                raise DomainError("STUDY_TARGET_OR_SPLIT_MISMATCH")
            for case in suite.cases:
                if c.execute("SELECT 1 FROM agent_workflows WHERE run_id=?", (case.episode_id,)).fetchone():
                    raise DomainError("HELDOUT_OVERLAPS_WORKFLOW_HISTORY")
            if c.execute("SELECT 1 FROM agent_policy_studies WHERE proposal_id=?",
                         (proposal.proposal_id,)).fetchone():
                raise DomainError("STUDY_ALREADY_RECORDED")
        # Backend executes OUTSIDE the SQLite writer transaction. Caller must
        # assure offline isolation; never run this in a live robotics context.
        report = self._report(proposal=proposal, suite=suite,
                              baseline=baseline, candidate=candidate, runner=runner)
        with self.store._write() as c:
            again = WorkflowEvolutionCycle._proposal(c, proposal_run_id)
            frozen = self._read_suite(c, suite_id)
            if again.content_hash != proposal.content_hash or frozen.digest != suite.digest:
                raise DomainError("STUDY_SOURCE_CHANGED")
            if c.execute("SELECT 1 FROM agent_policy_studies WHERE proposal_id=?",
                         (proposal.proposal_id,)).fetchone():
                raise DomainError("STUDY_ALREADY_RECORDED")
            study_id = uid()
            c.execute("INSERT INTO agent_policy_studies "
                      "(study_id,proposal_id,suite_id,baseline_hash,candidate_hash,report_json,report_hash,created_at) "
                      "VALUES (?,?,?,?,?,?,?,?)",
                      (study_id, proposal.proposal_id, suite_id, baseline.digest,
                       candidate.digest, canonical_bytes(report).decode("utf-8"),
                       content_hash(report), utc_now()))
            return self._read_study(c, study_id)

    def get_study(self, study_id: str) -> PolicyStudyRecord:
        with closing(self.store._connect()) as c:
            return self._read_study(c, study_id)
