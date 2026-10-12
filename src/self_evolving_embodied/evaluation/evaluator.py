"""R09a: isolated Fake evaluation evidence for *non-executable* R08 proposals.

The candidate arm deliberately runs the UNMODIFIED baseline. This assesses
regression harness reproducibility, NOT proposal efficacy; it can never
approve, publish, or mark a review-only candidate as performance-validated.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from ..adapters.fake.gateway import FakeGateway
from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError, Postcondition, TaskContract
from ..core.ids import uid, utc_now
from ..evolution import CandidateRecord, EvolutionCandidateGenerator
from ..runtime.orchestrator import FakeHarness
from ..runtime.planning import FakePlanner
from ..storage.sqlite.database import SqliteStore
from .frozen_suite import FROZEN_SUITE_ID, frozen_suite


@dataclass(frozen=True)
class EvaluationRecord:
    evaluation_id: str
    candidate_id: str
    baseline_release_id: str
    suite_id: str
    report: dict[str, Any]
    report_hash: str


class CandidateEvaluator:
    """Read-only Fake regression, immutable audit and fail-closed evaluation gate."""

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @staticmethod
    def _candidate(c: sqlite3.Connection, candidate_id: str) -> CandidateRecord:
        row = c.execute(
            "SELECT * FROM evolution_candidates WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        if row is None:
            raise DomainError("EVALUATION_CANDIDATE_NOT_FOUND")
        candidate = EvolutionCandidateGenerator._record(c, row)
        if (candidate.content.get("candidate_kind") != "REVIEW_ONLY_NON_EXECUTABLE"
            or candidate.content.get("apply_authorized") is not False
            or candidate.content.get("publish_authorized") is not False
            or candidate.content.get("requires_independent_heldout_evaluation") is not True):
            raise DomainError("EVALUATION_UNSUPPORTED_CANDIDATE")
        if candidate.baseline_release_id != "R0-FAKE":
            raise DomainError("EVALUATION_UNSUPPORTED_BASELINE")
        return candidate

    @staticmethod
    def _suite(c: sqlite3.Connection, *, create: bool = False) -> str:
        expected = frozen_suite()
        digest = content_hash(expected)
        value = canonical_bytes(expected).decode("utf-8")
        if create:
            c.execute(
                "INSERT OR IGNORE INTO evaluation_suites "
                "(suite_id,definition_json,definition_hash,created_at) VALUES (?,?,?,?)",
                (FROZEN_SUITE_ID, value, digest, utc_now()),
            )
        row = c.execute(
            "SELECT definition_json,definition_hash FROM evaluation_suites WHERE suite_id=?",
            (FROZEN_SUITE_ID,),
        ).fetchone()
        if row is None or row["definition_json"] != value or row["definition_hash"] != digest:
            raise DomainError("EVALUATION_SUITE_TAMPERED")
        return digest

    @staticmethod
    def _run_fake_case(case: dict[str, Any]) -> dict[str, Any]:
        """Execute one case in an isolated transient SQLite store and Fake Gateway."""
        with tempfile.TemporaryDirectory(prefix="r09a_fake_") as dirname:
            fresh = SqliteStore(Path(dirname) / "trial.sqlite3")
            gateway = FakeGateway(cast(str, case["scenario"]))
            task = TaskContract(
                task_id=uid(), goal=Postcondition("heldout-goal", "goal_reached", True),
                allowed_capabilities=("fake.move",), release_id="R0-FAKE",
            )
            planner = FakePlanner(tool_id=cast(str, case["tool_id"]))
            outcome = FakeHarness(fresh, gateway, planner).run(task)
            status = outcome.status.value
            return {
                "case_id": case["case_id"],
                "status": status,
                "invocations": len(gateway.invocations),
                "failure_code": outcome.failure_code,
                "oracle_match": (
                    status == case["expected_status"]
                    and len(gateway.invocations) == case["expected_invocations"]
                ),
                "quarantine_preserved": (
                    status != "HUMAN_REVIEW"
                    or fresh.table_count("resource_quarantine") > 0
                ),
            }

    @staticmethod
    def _make_report(candidate: CandidateRecord, suite_hash: str) -> dict[str, Any]:
        suite = frozen_suite()
        # Independent ephemeral databases for each arm and case, but no candidate
        # behavior can be applied because R08 proposals contain no executable delta.
        baseline = [CandidateEvaluator._run_fake_case(case) for case in suite["cases"]]
        proxy = [CandidateEvaluator._run_fake_case(case) for case in suite["cases"]]
        # Different case executions have different task/observation identifiers.
        # Those identifiers are deliberately not part of the metrics/digest.
        oracle_pass = all(r["oracle_match"] and r["quarantine_preserved"]
                          for r in baseline + proxy)
        no_regression = baseline == proxy
        n = len(baseline)
        baseline_wins = sum(r["status"] == "SUCCEEDED" for r in baseline)
        candidate_wins = sum(r["status"] == "SUCCEEDED" for r in proxy)
        return {
            "schema_version": 1,
            "evaluation_kind": "ISOLATED_FAKE_REGRESSION_EVIDENCE_ONLY",
            "candidate_id": candidate.candidate_id,
            "candidate_hash": candidate.content_hash,
            "source_task_id": candidate.task_id,
            "baseline_release_id": candidate.baseline_release_id,
            "baseline_release_hash": candidate.content["baseline_release_hash"],
            "suite_id": FROZEN_SUITE_ID,
            "suite_hash": suite_hash,
            "suite_kind": suite["suite_kind"],
            "independent_real_embodied_heldout": False,
            "training_scenario_family_overlap": True,
            "candidate_executable": False,
            "candidate_behavior_applied": False,
            "candidate_effect_measured": False,
            "arm_comparison": "BASELINE_VS_IDENTICAL_UNMODIFIED_FAKE_PROXY",
            "cases": n,
            "baseline_success_count": baseline_wins,
            "candidate_proxy_success_count": candidate_wins,
            "success_delta_count": candidate_wins - baseline_wins,
            "baseline_case_results": baseline,
            "candidate_proxy_case_results": proxy,
            "oracle_pass": oracle_pass,
            "no_regression_observed": no_regression,
            "evaluation_recorded": True,
            "performance_validated": False,
            "promotion_eligible": False,
            "gate_status": "BLOCKED",
            "blockers": [
                "REVIEW_ONLY_NOT_EXECUTABLE",
                "NO_CANDIDATE_BEHAVIOR_TO_COMPARE",
                "NO_INDEPENDENT_EMBODIED_HELDOUT_EVIDENCE",
                "HUMAN_APPROVAL_NOT_AVAILABLE",
            ],
            "limits": [
                "FAKE_SCENARIOS_NOT_REAL_EMBODIED_BENCHMARK",
                "NO_CAUSAL_IMPROVEMENT_CLAIM",
                "NO_RELEASE_CHANGE",
            ],
        }

    @classmethod
    def _record(cls, c: sqlite3.Connection, row: sqlite3.Row) -> EvaluationRecord:
        candidate = cls._candidate(c, cast(str, row["candidate_id"]))
        suite_hash = cls._suite(c)
        try:
            report = json.loads(cast(str, row["report_json"]))
            if not isinstance(report, dict):
                raise DomainError("EVALUATION_RECORD_TAMPERED")
            if (content_hash(report) != row["report_hash"]
                or canonical_bytes(report).decode("utf-8") != row["report_json"]
                or row["baseline_release_id"] != candidate.baseline_release_id
                or row["baseline_release_hash"] != candidate.content["baseline_release_hash"]
                or row["suite_id"] != FROZEN_SUITE_ID
                or row["suite_hash"] != suite_hash
                or report.get("candidate_id") != candidate.candidate_id
                or report.get("candidate_hash") != candidate.content_hash
                or report.get("baseline_release_hash") != row["baseline_release_hash"]
                or report.get("suite_hash") != suite_hash
                or report.get("gate_status") != "BLOCKED"
                or report.get("candidate_effect_measured") is not False
                or report.get("promotion_eligible") is not False
                or report.get("candidate_behavior_applied") is not False
                or report.get("performance_validated") is not False):
                raise DomainError("EVALUATION_RECORD_TAMPERED")
            return EvaluationRecord(
                cast(str, row["evaluation_id"]), candidate.candidate_id,
                candidate.baseline_release_id, FROZEN_SUITE_ID,
                cast(dict[str, Any], report), cast(str, row["report_hash"]),
            )
        except (TypeError, ValueError, KeyError) as exc:
            raise DomainError("EVALUATION_RECORD_TAMPERED") from exc

    def evaluate(self, candidate_id: str) -> EvaluationRecord:
        if not isinstance(candidate_id, str) or not candidate_id:
            raise DomainError("INVALID_EVALUATION_QUERY")
        with self.store._write() as c:
            candidate = self._candidate(c, candidate_id)
            digest = self._suite(c, create=True)
            existing = c.execute(
                "SELECT * FROM candidate_evaluations WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            if existing is not None:
                return self._record(c, existing)
        # Must never execute external actions (even Fake) inside SQLite transactions.
        report = self._make_report(candidate, digest)
        with self.store._write() as c:
            current = self._candidate(c, candidate_id)  # revalidate provenance and baseline
            if current != candidate or self._suite(c) != digest:
                raise DomainError("EVALUATION_SOURCE_CHANGED")
            existing = c.execute(
                "SELECT * FROM candidate_evaluations WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            if existing is not None:
                return self._record(c, existing)
            c.execute(
                "INSERT INTO candidate_evaluations "
                "(evaluation_id,candidate_id,baseline_release_id,baseline_release_hash,"
                "suite_id,suite_hash,report_json,report_hash,evaluated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (uid(), candidate_id, candidate.baseline_release_id,
                 candidate.content["baseline_release_hash"], FROZEN_SUITE_ID, digest,
                 canonical_bytes(report).decode("utf-8"), content_hash(report), utc_now()),
            )
            row = c.execute(
                "SELECT * FROM candidate_evaluations WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            if row is None:
                raise DomainError("EVALUATION_WRITE_FAILED")
            return self._record(c, row)

    def get(self, candidate_id: str) -> EvaluationRecord | None:
        if not isinstance(candidate_id, str) or not candidate_id:
            raise DomainError("INVALID_EVALUATION_QUERY")
        with closing(self.store._connect()) as c:
            row = c.execute(
                "SELECT * FROM candidate_evaluations WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            return None if row is None else self._record(c, row)

    def search(self, *, release_id: str, limit: int = 100) -> list[EvaluationRecord]:
        if (not isinstance(release_id, str) or not release_id
            or type(limit) is not int or not 1 <= limit <= 1000):
            raise DomainError("INVALID_EVALUATION_QUERY")
        with closing(self.store._connect()) as c:
            rows = c.execute(
                "SELECT * FROM candidate_evaluations WHERE baseline_release_id=? "
                "ORDER BY evaluated_at,candidate_id LIMIT ?", (release_id, limit),
            ).fetchall()
            return [self._record(c, row) for row in rows]
