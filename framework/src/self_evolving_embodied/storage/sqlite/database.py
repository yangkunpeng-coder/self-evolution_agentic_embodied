"""Single local SQLite writer domain for the Step 6 Fake MVP.

No external calls, awaits, or model inference may run inside BEGIN IMMEDIATE.
Physical side-effects cannot be rolled back by this database.
"""
from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

from ...core.canonical_json import canonical_bytes, content_hash
from ...core.contracts import (
    AuthorizedInvocation,
    DomainError,
    EvidenceBundle,
    PlanStep,
    ProviderResponse,
    Status,
    TaskContract,
    TaskOutcome,
    Verdict,
    VerificationVerdict,
)
from ...core.ids import uid, utc_now
from ...harness.skill_models import R0_SKILL
from ...harness.verifier import IndependentVerifier, evidence_digest

R0_MANIFEST = {"release_id": "R0-FAKE", "capabilities": ["fake.move"],
               "verifier": IndependentVerifier.ID, "candidate_publish_enabled": False,
               "provider": "deterministic-fake-v1", "schema_version": 1}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS release_manifests (
 release_id TEXT PRIMARY KEY, body TEXT NOT NULL, body_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS active_release (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), release_id TEXT NOT NULL,
 FOREIGN KEY(release_id) REFERENCES release_manifests(release_id));
CREATE TRIGGER IF NOT EXISTS immutable_manifest_update BEFORE UPDATE ON release_manifests
BEGIN SELECT RAISE(ABORT, 'release manifest immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_manifest_delete BEFORE DELETE ON release_manifests
BEGIN SELECT RAISE(ABORT, 'release manifest immutable'); END;
CREATE TABLE IF NOT EXISTS tasks (
 task_id TEXT PRIMARY KEY, release_id TEXT NOT NULL, release_hash TEXT NOT NULL,
 goal_json TEXT NOT NULL, status TEXT NOT NULL, state_version INTEGER NOT NULL,
 event_seq INTEGER NOT NULL DEFAULT 0, last_event_hash TEXT NOT NULL DEFAULT '',
 max_steps INTEGER NOT NULL, max_retries INTEGER NOT NULL, created_at TEXT NOT NULL,
 FOREIGN KEY(release_id) REFERENCES release_manifests(release_id));
CREATE TABLE IF NOT EXISTS events (
 event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, seq INTEGER NOT NULL,
 kind TEXT NOT NULL, payload TEXT NOT NULL, previous_hash TEXT NOT NULL,
 event_hash TEXT NOT NULL, at TEXT NOT NULL,
 UNIQUE(task_id,seq), FOREIGN KEY(task_id) REFERENCES tasks(task_id));
CREATE TABLE IF NOT EXISTS invocations (
 invocation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, step_id TEXT NOT NULL,
 release_id TEXT NOT NULL, tool_id TEXT NOT NULL, arguments_hash TEXT NOT NULL,
 step_condition_json TEXT NOT NULL, intent_state_version INTEGER NOT NULL,
 status TEXT NOT NULL, provider_status TEXT, resource TEXT NOT NULL,
 FOREIGN KEY(task_id) REFERENCES tasks(task_id));
CREATE TABLE IF NOT EXISTS evidences (
 evidence_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, invocation_id TEXT NOT NULL,
 scope TEXT NOT NULL, payload TEXT NOT NULL, evidence_hash TEXT NOT NULL,
 FOREIGN KEY(task_id) REFERENCES tasks(task_id));
CREATE TABLE IF NOT EXISTS verdicts (
 verdict_id TEXT PRIMARY KEY, evidence_id TEXT NOT NULL, task_id TEXT NOT NULL,
 payload TEXT NOT NULL, verdict_hash TEXT NOT NULL,
 FOREIGN KEY(evidence_id) REFERENCES evidences(evidence_id));
CREATE TABLE IF NOT EXISTS commit_records (
 commit_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, scope TEXT NOT NULL,
 step_id TEXT, evidence_id TEXT NOT NULL, verdict_id TEXT NOT NULL,
 idempotency_key TEXT NOT NULL, request_hash TEXT NOT NULL,
 previous_state_version INTEGER NOT NULL, new_state_version INTEGER NOT NULL,
 committed_at TEXT NOT NULL, UNIQUE(task_id,idempotency_key),
 FOREIGN KEY(task_id) REFERENCES tasks(task_id));
CREATE TABLE IF NOT EXISTS outbox (
 message_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
 topic TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL,
 consumed INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS resource_quarantine (
 resource TEXT PRIMARY KEY, invocation_id TEXT NOT NULL, reason TEXT NOT NULL,
 created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS stop_requests (
 invocation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
 reason TEXT NOT NULL, acknowledgement TEXT NOT NULL DEFAULT 'PENDING',
 requested_at TEXT NOT NULL, acknowledged_at TEXT,
 FOREIGN KEY(invocation_id) REFERENCES invocations(invocation_id));
CREATE TABLE IF NOT EXISTS task_cancellations (
 task_id TEXT PRIMARY KEY, requested_at TEXT NOT NULL,
 FOREIGN KEY(task_id) REFERENCES tasks(task_id));
CREATE TABLE IF NOT EXISTS worker_lease (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 owner_id TEXT, fence_token INTEGER NOT NULL,
 expires_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS memory_records (
 memory_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
 trust_level TEXT NOT NULL CHECK(trust_level IN ('RAW','VERIFIED')),
 content_json TEXT NOT NULL, content_hash TEXT NOT NULL,
 source_task_id TEXT NOT NULL, source_commit_id TEXT UNIQUE,
 source_evidence_id TEXT, source_verdict_id TEXT,
 release_id TEXT NOT NULL, scope_task_id TEXT NOT NULL,
 expires_at_ms INTEGER, created_at TEXT NOT NULL,
 provenance TEXT NOT NULL,
 FOREIGN KEY(source_task_id) REFERENCES tasks(task_id),
 FOREIGN KEY(source_commit_id) REFERENCES commit_records(commit_id));
CREATE TRIGGER IF NOT EXISTS immutable_memory_update BEFORE UPDATE ON memory_records
BEGIN SELECT RAISE(ABORT, 'memory record immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_memory_delete BEFORE DELETE ON memory_records
BEGIN SELECT RAISE(ABORT, 'memory record immutable'); END;
CREATE TABLE IF NOT EXISTS memory_revocations (
 memory_id TEXT PRIMARY KEY, reason TEXT NOT NULL, revoked_at TEXT NOT NULL,
 FOREIGN KEY(memory_id) REFERENCES memory_records(memory_id));
CREATE TRIGGER IF NOT EXISTS immutable_memory_revoke_update BEFORE UPDATE ON memory_revocations
BEGIN SELECT RAISE(ABORT, 'memory revocation immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_memory_revoke_delete BEFORE DELETE ON memory_revocations
BEGIN SELECT RAISE(ABORT, 'memory revocation immutable'); END;
CREATE TABLE IF NOT EXISTS memory_audit (
 event_id TEXT PRIMARY KEY, memory_id TEXT NOT NULL, event_type TEXT NOT NULL,
 details TEXT NOT NULL, recorded_at TEXT NOT NULL,
 FOREIGN KEY(memory_id) REFERENCES memory_records(memory_id));
CREATE TRIGGER IF NOT EXISTS immutable_memory_audit_update BEFORE UPDATE ON memory_audit
BEGIN SELECT RAISE(ABORT, 'memory audit immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_memory_audit_delete BEFORE DELETE ON memory_audit
BEGIN SELECT RAISE(ABORT, 'memory audit immutable'); END;
CREATE INDEX IF NOT EXISTS idx_memory_scope ON memory_records(scope_task_id,release_id,trust_level);
CREATE TABLE IF NOT EXISTS schema_meta (version INTEGER NOT NULL);
"""


_SKILL_SCHEMA = """
CREATE TABLE IF NOT EXISTS skill_definitions (
 skill_id TEXT NOT NULL, version INTEGER NOT NULL, release_id TEXT NOT NULL,
 tool_id TEXT NOT NULL, definition_json TEXT NOT NULL, definition_hash TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(skill_id,version),
 FOREIGN KEY(release_id) REFERENCES release_manifests(release_id));
CREATE TRIGGER IF NOT EXISTS immutable_skill_definition_update BEFORE UPDATE ON skill_definitions
BEGIN SELECT RAISE(ABORT, 'skill definition immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_skill_definition_delete BEFORE DELETE ON skill_definitions
BEGIN SELECT RAISE(ABORT, 'skill definition immutable'); END;
CREATE TABLE IF NOT EXISTS skill_states (
 skill_id TEXT NOT NULL, version INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('DRAFT','VALIDATED','PUBLISHED','DEPRECATED')),
 PRIMARY KEY(skill_id,version),
 FOREIGN KEY(skill_id,version) REFERENCES skill_definitions(skill_id,version));
CREATE TRIGGER IF NOT EXISTS skill_lifecycle_guard BEFORE UPDATE OF state ON skill_states
WHEN NOT ((OLD.state='DRAFT' AND NEW.state IN ('VALIDATED','DEPRECATED'))
 OR (OLD.state='VALIDATED' AND NEW.state='DEPRECATED'))
BEGIN SELECT RAISE(ABORT, 'invalid skill lifecycle transition'); END;
CREATE TRIGGER IF NOT EXISTS immutable_skill_state_delete BEFORE DELETE ON skill_states
BEGIN SELECT RAISE(ABORT, 'skill lifecycle immutable'); END;
CREATE TABLE IF NOT EXISTS release_skill_bindings (
 release_id TEXT NOT NULL, tool_id TEXT NOT NULL,
 skill_id TEXT NOT NULL, version INTEGER NOT NULL, definition_hash TEXT NOT NULL,
 PRIMARY KEY(release_id,tool_id),
 FOREIGN KEY(release_id) REFERENCES release_manifests(release_id),
 FOREIGN KEY(skill_id,version) REFERENCES skill_definitions(skill_id,version));
CREATE TRIGGER IF NOT EXISTS immutable_skill_binding_update BEFORE UPDATE ON release_skill_bindings
BEGIN SELECT RAISE(ABORT, 'skill binding immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_skill_binding_delete BEFORE DELETE ON release_skill_bindings
BEGIN SELECT RAISE(ABORT, 'skill binding immutable'); END;
CREATE TABLE IF NOT EXISTS skill_audit (
 event_id TEXT PRIMARY KEY, skill_id TEXT NOT NULL, version INTEGER NOT NULL,
 event_type TEXT NOT NULL, details TEXT NOT NULL, recorded_at TEXT NOT NULL,
 FOREIGN KEY(skill_id,version) REFERENCES skill_definitions(skill_id,version));
CREATE TRIGGER IF NOT EXISTS immutable_skill_audit_update BEFORE UPDATE ON skill_audit
BEGIN SELECT RAISE(ABORT, 'skill audit immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_skill_audit_delete BEFORE DELETE ON skill_audit
BEGIN SELECT RAISE(ABORT, 'skill audit immutable'); END;
CREATE INDEX IF NOT EXISTS idx_skill_release ON skill_definitions(release_id,tool_id);
"""


_EXPERIENCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS experience_records (
 experience_id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE,
 release_id TEXT NOT NULL, terminal_status TEXT NOT NULL
 CHECK(terminal_status IN ('SUCCEEDED','FAILED','HUMAN_REVIEW')),
 trust_level TEXT NOT NULL CHECK(trust_level IN ('VERIFIED','UNVERIFIED','UNCERTAIN')),
 content_json TEXT NOT NULL, content_hash TEXT NOT NULL,
 event_head_hash TEXT NOT NULL, projected_at TEXT NOT NULL,
 FOREIGN KEY(task_id) REFERENCES tasks(task_id));
CREATE TRIGGER IF NOT EXISTS immutable_experience_update BEFORE UPDATE ON experience_records
BEGIN SELECT RAISE(ABORT, 'experience immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_experience_delete BEFORE DELETE ON experience_records
BEGIN SELECT RAISE(ABORT, 'experience immutable'); END;
CREATE INDEX IF NOT EXISTS idx_experience_release ON
 experience_records(release_id,terminal_status,trust_level);
"""


_DIAGNOSIS_SCHEMA = """
CREATE TABLE IF NOT EXISTS diagnosis_records (
 diagnosis_id TEXT PRIMARY KEY, experience_id TEXT NOT NULL UNIQUE,
 task_id TEXT NOT NULL UNIQUE, release_id TEXT NOT NULL,
 category TEXT NOT NULL, source_experience_hash TEXT NOT NULL,
 content_json TEXT NOT NULL, content_hash TEXT NOT NULL,
 diagnosed_at TEXT NOT NULL,
 FOREIGN KEY(experience_id) REFERENCES experience_records(experience_id),
 FOREIGN KEY(task_id) REFERENCES tasks(task_id));
CREATE TRIGGER IF NOT EXISTS immutable_diagnosis_update BEFORE UPDATE ON diagnosis_records
BEGIN SELECT RAISE(ABORT, 'diagnosis immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_diagnosis_delete BEFORE DELETE ON diagnosis_records
BEGIN SELECT RAISE(ABORT, 'diagnosis immutable'); END;
CREATE INDEX IF NOT EXISTS idx_diagnosis_release ON diagnosis_records(release_id,category);
"""



_CANDIDATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS evolution_candidates (
 candidate_id TEXT PRIMARY KEY,
 diagnosis_id TEXT NOT NULL UNIQUE,
 task_id TEXT NOT NULL UNIQUE,
 baseline_release_id TEXT NOT NULL,
 target TEXT NOT NULL CHECK(target IN ('PLANNING_PROMPT','SKILL_ROUTING','WORKFLOW')),
 template_id TEXT NOT NULL,
 content_json TEXT NOT NULL, content_hash TEXT NOT NULL,
 created_at TEXT NOT NULL,
 FOREIGN KEY(diagnosis_id) REFERENCES diagnosis_records(diagnosis_id),
 FOREIGN KEY(task_id) REFERENCES tasks(task_id),
 FOREIGN KEY(baseline_release_id) REFERENCES release_manifests(release_id));
CREATE TRIGGER IF NOT EXISTS immutable_candidate_update BEFORE UPDATE ON evolution_candidates
BEGIN SELECT RAISE(ABORT, 'candidate immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_candidate_delete BEFORE DELETE ON evolution_candidates
BEGIN SELECT RAISE(ABORT, 'candidate immutable'); END;
CREATE TABLE IF NOT EXISTS candidate_decisions (
 diagnosis_id TEXT PRIMARY KEY, source_diagnosis_hash TEXT NOT NULL,
 decision TEXT NOT NULL CHECK(decision IN ('PROPOSED','SKIPPED_UNSAFE_OR_UNSUPPORTED')),
 decided_at TEXT NOT NULL,
 FOREIGN KEY(diagnosis_id) REFERENCES diagnosis_records(diagnosis_id));
CREATE TRIGGER IF NOT EXISTS immutable_candidate_decision_update
BEFORE UPDATE ON candidate_decisions
BEGIN SELECT RAISE(ABORT, 'candidate decision immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_candidate_decision_delete
BEFORE DELETE ON candidate_decisions
BEGIN SELECT RAISE(ABORT, 'candidate decision immutable'); END;
CREATE INDEX IF NOT EXISTS idx_candidate_release_target
ON evolution_candidates(baseline_release_id,target);
"""


_EVALUATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS evaluation_suites (
 suite_id TEXT PRIMARY KEY, definition_json TEXT NOT NULL,
 definition_hash TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS immutable_eval_suite_update BEFORE UPDATE ON evaluation_suites
BEGIN SELECT RAISE(ABORT, 'evaluation suite immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_eval_suite_delete BEFORE DELETE ON evaluation_suites
BEGIN SELECT RAISE(ABORT, 'evaluation suite immutable'); END;
CREATE TABLE IF NOT EXISTS candidate_evaluations (
 evaluation_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL UNIQUE,
 baseline_release_id TEXT NOT NULL, baseline_release_hash TEXT NOT NULL,
 suite_id TEXT NOT NULL, suite_hash TEXT NOT NULL,
 report_json TEXT NOT NULL, report_hash TEXT NOT NULL,
 evaluated_at TEXT NOT NULL,
 FOREIGN KEY(candidate_id) REFERENCES evolution_candidates(candidate_id),
 FOREIGN KEY(baseline_release_id) REFERENCES release_manifests(release_id),
 FOREIGN KEY(suite_id) REFERENCES evaluation_suites(suite_id));
CREATE TRIGGER IF NOT EXISTS immutable_eval_result_update
BEFORE UPDATE ON candidate_evaluations
BEGIN SELECT RAISE(ABORT, 'evaluation record immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_eval_result_delete
BEFORE DELETE ON candidate_evaluations
BEGIN SELECT RAISE(ABORT, 'evaluation record immutable'); END;
CREATE INDEX IF NOT EXISTS idx_evaluation_release
ON candidate_evaluations(baseline_release_id);
"""


_REVIEW_SCHEMA = """
CREATE TABLE IF NOT EXISTS candidate_review_requests (
 request_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL UNIQUE,
 evaluation_id TEXT NOT NULL, evaluation_hash TEXT NOT NULL,
 baseline_release_id TEXT NOT NULL, baseline_release_hash TEXT NOT NULL,
 request_json TEXT NOT NULL, request_hash TEXT NOT NULL,
 requested_at TEXT NOT NULL,
 FOREIGN KEY(candidate_id) REFERENCES evolution_candidates(candidate_id),
 FOREIGN KEY(evaluation_id) REFERENCES candidate_evaluations(evaluation_id));
CREATE TRIGGER IF NOT EXISTS immutable_review_request_update
BEFORE UPDATE ON candidate_review_requests
BEGIN SELECT RAISE(ABORT,'review request immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_review_request_delete
BEFORE DELETE ON candidate_review_requests
BEGIN SELECT RAISE(ABORT,'review request immutable'); END;
CREATE TABLE IF NOT EXISTS candidate_review_decisions (
 decision_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
 reviewer_id TEXT NOT NULL, choice TEXT NOT NULL
 CHECK(choice IN ('ACCEPT_FOR_RESEARCH','REJECT')),
 decision_json TEXT NOT NULL, decision_hash TEXT NOT NULL,
 decided_at TEXT NOT NULL,
 FOREIGN KEY(request_id) REFERENCES candidate_review_requests(request_id));
CREATE TRIGGER IF NOT EXISTS immutable_review_decision_update
BEFORE UPDATE ON candidate_review_decisions
BEGIN SELECT RAISE(ABORT,'review decision immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_review_decision_delete
BEFORE DELETE ON candidate_review_decisions
BEGIN SELECT RAISE(ABORT,'review decision immutable'); END;
CREATE INDEX IF NOT EXISTS idx_review_release ON
candidate_review_requests(baseline_release_id);
"""


_RELEASE_MANAGEMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS release_pointer_state (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 release_id TEXT NOT NULL, manifest_hash TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision >= 0),
 FOREIGN KEY(release_id) REFERENCES release_manifests(release_id));
CREATE TABLE IF NOT EXISTS staged_release_manifests (
 staged_release_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL UNIQUE,
 baseline_release_id TEXT NOT NULL, baseline_release_hash TEXT NOT NULL,
 manifest_json TEXT NOT NULL, manifest_hash TEXT NOT NULL,
 created_at TEXT NOT NULL,
 FOREIGN KEY(candidate_id) REFERENCES evolution_candidates(candidate_id));
CREATE TRIGGER IF NOT EXISTS immutable_staged_release_update
BEFORE UPDATE ON staged_release_manifests
BEGIN SELECT RAISE(ABORT,'staged release immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_staged_release_delete
BEFORE DELETE ON staged_release_manifests
BEGIN SELECT RAISE(ABORT,'staged release immutable'); END;
CREATE TABLE IF NOT EXISTS release_activation_attempts (
 attempt_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
 staged_release_id TEXT NOT NULL,
 expected_release_id TEXT NOT NULL, expected_revision INTEGER NOT NULL,
 attempt_json TEXT NOT NULL, attempt_hash TEXT NOT NULL,
 attempted_at TEXT NOT NULL,
 FOREIGN KEY(staged_release_id) REFERENCES staged_release_manifests(staged_release_id));
CREATE TRIGGER IF NOT EXISTS immutable_activation_attempt_update
BEFORE UPDATE ON release_activation_attempts
BEGIN SELECT RAISE(ABORT,'release activation audit immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_activation_attempt_delete
BEFORE DELETE ON release_activation_attempts
BEGIN SELECT RAISE(ABORT,'release activation audit immutable'); END;
"""


_ROLLBACK_SCHEMA = """
CREATE TABLE IF NOT EXISTS release_rollback_attempts (
 attempt_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
 target_release_id TEXT NOT NULL, expected_release_id TEXT NOT NULL,
 expected_manifest_hash TEXT NOT NULL, expected_revision INTEGER NOT NULL,
 rationale TEXT NOT NULL, attempt_json TEXT NOT NULL, attempt_hash TEXT NOT NULL,
 attempted_at TEXT NOT NULL,
 FOREIGN KEY(target_release_id) REFERENCES release_manifests(release_id));
CREATE TRIGGER IF NOT EXISTS immutable_rollback_attempt_update
BEFORE UPDATE ON release_rollback_attempts
BEGIN SELECT RAISE(ABORT,'rollback audit immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_rollback_attempt_delete
BEFORE DELETE ON release_rollback_attempts
BEGIN SELECT RAISE(ABORT,'rollback audit immutable'); END;
"""


@dataclass(frozen=True)
class WorkerFence:
    """Monotonic database epoch. Never a substitute for device-side fencing."""

    owner_id: str
    token: int
    lease_ms: int


class SqliteStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self._fence: ContextVar[WorkerFence | None] = ContextVar(
            "sqlite_r03_worker_fence", default=None,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA busy_timeout=10000")
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")
        return c

    @contextmanager
    def _write(self, *, external_cancel: bool = False) -> Iterator[sqlite3.Connection]:
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            if not external_cancel:
                self._require_current_fence(c)
            yield c
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise
        finally:
            c.close()

    @staticmethod
    def _execute_schema(c: sqlite3.Connection, script: str) -> None:
        """Execute complete DDL statements without executescript()'s implicit COMMIT.

        sqlite3.complete_statement respects trigger BEGIN/END blocks, unlike
        naive splitting on semicolons. Every statement stays in the caller's
        BEGIN IMMEDIATE transaction so a failed upgrade cannot half-apply.
        """
        statement = ""
        for line in script.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                c.execute(statement)
                statement = ""
        if statement.strip():
            raise DomainError("INCOMPLETE_SCHEMA_DDL")

    def _initialize(self) -> None:
        with closing(self._connect()) as c:
            try:
                c.execute("BEGIN IMMEDIATE")
                # Do not change a database whose version cannot be understood.
                # Existing databases without metadata are not safe to bootstrap.
                meta_exists = c.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_meta'"
                ).fetchone() is not None
                if meta_exists:
                    found = c.execute("SELECT version FROM schema_meta").fetchall()
                    if (len(found) != 1 or not isinstance(found[0][0], int)
                        or found[0][0] not in range(1, 13)):
                        raise DomainError("UNSUPPORTED_SCHEMA")
                else:
                    has_tables = c.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name NOT LIKE 'sqlite_%' LIMIT 1"
                    ).fetchone() is not None
                    if has_tables:
                        raise DomainError("SCHEMA_METADATA_MISSING")
                    found = []
                # Validate before applying any DDL, and recheck FK constraints
                # after migration. This catches common accidental corruption,
                # not malicious writers able to replace/re-hash the database.
                if c.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise DomainError("SQLITE_INTEGRITY_FAILED")
                if c.execute("PRAGMA foreign_key_check").fetchone() is not None:
                    raise DomainError("SQLITE_FOREIGN_KEY_VIOLATION")
                for script in (
                    _SCHEMA, _SKILL_SCHEMA, _EXPERIENCE_SCHEMA, _DIAGNOSIS_SCHEMA,
                    _CANDIDATE_SCHEMA, _EVALUATION_SCHEMA, _REVIEW_SCHEMA,
                    _RELEASE_MANAGEMENT_SCHEMA, _ROLLBACK_SCHEMA,
                ):
                    self._execute_schema(c, script)
                if not found:
                    c.execute("INSERT INTO schema_meta VALUES (12)")
                elif found[0][0] < 12:
                    c.execute("UPDATE schema_meta SET version=12")
                c.execute("INSERT OR IGNORE INTO worker_lease VALUES (1,NULL,0,0)")
                body = canonical_bytes(R0_MANIFEST).decode("utf-8")
                digest = content_hash(R0_MANIFEST)
                c.execute("INSERT OR IGNORE INTO release_manifests VALUES (?,?,?)",
                          ("R0-FAKE", body, digest))
                saved = c.execute("SELECT * FROM release_manifests WHERE release_id='R0-FAKE'").fetchone()
                if saved is None or saved["body"] != body or saved["body_hash"] != digest:
                    raise DomainError("RELEASE_TAMPERED")
                c.execute("INSERT OR IGNORE INTO active_release VALUES (1,'R0-FAKE')")
                active = c.execute("SELECT release_id FROM active_release WHERE singleton=1").fetchone()
                if active is None or active[0] != "R0-FAKE":
                    raise DomainError("UNTRUSTED_ACTIVE_RELEASE")
                c.execute(
                    "INSERT OR IGNORE INTO release_pointer_state VALUES (1,?,?,0)",
                    ("R0-FAKE", digest),
                )
                pointer = c.execute(
                    "SELECT release_id,manifest_hash,revision FROM release_pointer_state "
                    "WHERE singleton=1"
                ).fetchone()
                if (pointer is None or pointer["release_id"] != "R0-FAKE"
                    or pointer["manifest_hash"] != digest or pointer["revision"] != 0):
                    raise DomainError("UNTRUSTED_RELEASE_POINTER")
                # Trusted one-time seed: the only skill executable under immutable R0.
                spec_body = canonical_bytes(asdict(R0_SKILL)).decode("utf-8")
                c.execute(
                    "INSERT OR IGNORE INTO skill_definitions VALUES (?,?,?,?,?,?,?)",
                    (R0_SKILL.skill_id, R0_SKILL.version, R0_SKILL.release_id,
                     R0_SKILL.tool_id, spec_body, R0_SKILL.digest, utc_now()),
                )
                builtin = c.execute(
                    "SELECT definition_json,definition_hash FROM skill_definitions "
                    "WHERE skill_id=? AND version=?", (R0_SKILL.skill_id, R0_SKILL.version),
                ).fetchone()
                if (builtin is None or builtin["definition_json"] != spec_body
                    or builtin["definition_hash"] != R0_SKILL.digest):
                    raise DomainError("BUILTIN_SKILL_TAMPERED")
                c.execute("INSERT OR IGNORE INTO skill_states VALUES (?,?,?)",
                          (R0_SKILL.skill_id, R0_SKILL.version, "PUBLISHED"))
                state = c.execute("SELECT state FROM skill_states WHERE skill_id=? AND version=?",
                                  (R0_SKILL.skill_id, R0_SKILL.version)).fetchone()
                if state is None or state["state"] != "PUBLISHED":
                    raise DomainError("BUILTIN_SKILL_UNPUBLISHED")
                c.execute("INSERT OR IGNORE INTO release_skill_bindings VALUES (?,?,?,?,?)",
                          (R0_SKILL.release_id, R0_SKILL.tool_id, R0_SKILL.skill_id,
                           R0_SKILL.version, R0_SKILL.digest))
                binding = c.execute(
                    "SELECT skill_id,version,definition_hash FROM release_skill_bindings "
                    "WHERE release_id=? AND tool_id=?", (R0_SKILL.release_id, R0_SKILL.tool_id),
                ).fetchone()
                if (binding is None or binding["skill_id"] != R0_SKILL.skill_id
                    or binding["version"] != R0_SKILL.version
                    or binding["definition_hash"] != R0_SKILL.digest):
                    raise DomainError("BUILTIN_SKILL_BINDING_TAMPERED")
                c.execute(
                    "INSERT OR IGNORE INTO skill_audit VALUES (?,?,?,?,?,?)",
                    ("R05-R0-BOOTSTRAP", R0_SKILL.skill_id, R0_SKILL.version,
                     "SKILL_BOOTSTRAPPED", canonical_bytes({"digest": R0_SKILL.digest}).decode("utf-8"),
                     utc_now()),
                )
                if c.execute("PRAGMA foreign_key_check").fetchone() is not None:
                    raise DomainError("SQLITE_FOREIGN_KEY_VIOLATION")
                c.execute("COMMIT")
            except BaseException:
                if c.in_transaction:
                    c.execute("ROLLBACK")
                raise

    @staticmethod
    def _now_ms() -> int:
        return time.time_ns() // 1_000_000

    def _require_current_fence(self, c: sqlite3.Connection) -> None:
        """Called inside each mutating transaction, *before* any state change.

        A legacy unfenced writer is allowed only while the lease is explicitly
        unowned (for R01/R02 migration and offline administrative tests). A
        stolen or expired lease cannot be used for writes, even idempotent ones.
        """
        active = c.execute(
            "SELECT owner_id,fence_token,expires_at_ms FROM worker_lease "
            "WHERE singleton=1"
        ).fetchone()
        if active is None:
            raise DomainError("LEASE_SCHEMA_MISSING")
        fence = self._fence.get()
        if fence is None:
            if active["owner_id"] is not None:
                raise DomainError("WORKER_FENCE_REQUIRED")
            return
        now = self._now_ms()
        if (active["owner_id"] != fence.owner_id
            or active["fence_token"] != fence.token
            or active["expires_at_ms"] <= now):
            raise DomainError("STALE_WORKER_FENCE")
        # Cheap renewal in the SAME transaction; no gap between validating
        # the owner and committing the protected state transition.
        c.execute(
            "UPDATE worker_lease SET expires_at_ms=? "
            "WHERE singleton=1 AND owner_id=? AND fence_token=?",
            (now + fence.lease_ms, fence.owner_id, fence.token),
        )

    def acquire_worker(self, owner_id: str, lease_ms: int = 10_000) -> WorkerFence:
        """Atomically acquire/replace an idle or expired single-worker lease."""
        if not owner_id or not owner_id.strip() or lease_ms < 50:
            raise DomainError("INVALID_WORKER_LEASE")
        with closing(self._connect()) as c:
            try:
                c.execute("BEGIN IMMEDIATE")
                existing = c.execute(
                    "SELECT owner_id,fence_token,expires_at_ms FROM worker_lease "
                    "WHERE singleton=1"
                ).fetchone()
                if existing is None:
                    raise DomainError("LEASE_SCHEMA_MISSING")
                now = self._now_ms()
                if existing["owner_id"] is not None and existing["expires_at_ms"] > now:
                    raise DomainError("WORKER_LEASE_HELD")
                # Every acquisition advances the epoch, including after a clean exit.
                token = cast(int, existing["fence_token"]) + 1
                c.execute(
                    "UPDATE worker_lease SET owner_id=?,fence_token=?,expires_at_ms=? "
                    "WHERE singleton=1",
                    (owner_id, token, now + lease_ms),
                )
                c.execute("COMMIT")
                return WorkerFence(owner_id, token, lease_ms)
            except BaseException:
                c.execute("ROLLBACK")
                raise

    def release_worker(self, fence: WorkerFence) -> bool:
        """Stale owners cannot clear a newer owner's lease."""
        with closing(self._connect()) as c:
            try:
                c.execute("BEGIN IMMEDIATE")
                count = c.execute(
                    "UPDATE worker_lease SET owner_id=NULL, expires_at_ms=0 "
                    "WHERE singleton=1 AND owner_id=? AND fence_token=?",
                    (fence.owner_id, fence.token),
                ).rowcount
                c.execute("COMMIT")
                return count == 1
            except BaseException:
                c.execute("ROLLBACK")
                raise

    def heartbeat_worker(self) -> None:
        """Extend lease while awaiting an external Fake call, or fail closed."""
        if self._fence.get() is None:
            raise DomainError("WORKER_FENCE_REQUIRED")
        with self._write():
            pass

    def active_fencing_token(self) -> int:
        """Return current context epoch; callers pass it in adapter tickets."""
        fence = self._fence.get()
        if fence is None:
            raise DomainError("WORKER_FENCE_REQUIRED")
        with closing(self._connect()) as c:
            row = c.execute(
                "SELECT owner_id,fence_token,expires_at_ms FROM worker_lease "
                "WHERE singleton=1"
            ).fetchone()
            if (row is None or row["owner_id"] != fence.owner_id
                or row["fence_token"] != fence.token
                or row["expires_at_ms"] <= self._now_ms()):
                raise DomainError("STALE_WORKER_FENCE")
        return fence.token

    def lease_info(self) -> dict[str, object]:
        with closing(self._connect()) as c:
            row = c.execute("SELECT * FROM worker_lease WHERE singleton=1").fetchone()
            if row is None:
                raise DomainError("LEASE_SCHEMA_MISSING")
            return dict(row)

    @contextmanager
    def worker_session(self, owner_id: str | None = None,
                       lease_ms: int = 10_000) -> Iterator[WorkerFence]:
        """Single active FakeHarness; release only if still the current owner."""
        fence = self.acquire_worker(owner_id or uid(), lease_ms)
        ctx = self._fence.set(fence)
        try:
            yield fence
        finally:
            try:
                self.release_worker(fence)
            finally:
                self._fence.reset(ctx)

    @staticmethod
    def _task(c: sqlite3.Connection, task_id: str) -> sqlite3.Row:
        task = c.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if task is None:
            raise DomainError("TASK_NOT_FOUND")
        return cast(sqlite3.Row, task)

    @staticmethod
    def _verify_event_chain(c: sqlite3.Connection, task: sqlite3.Row) -> None:
        """Detect accidental history edits before extending the event chain.

        This is local corruption detection, NOT tamper-proof authentication:
        an attacker with database write access could rebuild the hashes.
        """
        expected_seq = 0
        previous_hash = ""
        rows = c.execute(
            "SELECT seq,kind,payload,previous_hash,event_hash FROM events "
            "WHERE task_id=? ORDER BY seq", (task["task_id"],),
        )
        for row in rows:
            expected_seq += 1
            if row["seq"] != expected_seq or row["previous_hash"] != previous_hash:
                raise DomainError("EVENT_CHAIN_INVALID")
            try:
                payload = json.loads(cast(str, row["payload"]))
                if (not isinstance(payload, dict)
                    or row["payload"] != canonical_bytes(payload).decode("utf-8")):
                    raise DomainError("EVENT_CHAIN_INVALID")
                data = {
                    "task_id": task["task_id"], "seq": expected_seq,
                    "kind": row["kind"], "payload": payload,
                    "previous_hash": previous_hash,
                }
                digest = content_hash(data)
            except (ValueError, TypeError) as exc:
                raise DomainError("EVENT_CHAIN_INVALID") from exc
            if digest != row["event_hash"]:
                raise DomainError("EVENT_CHAIN_INVALID")
            previous_hash = digest
        if task["event_seq"] != expected_seq or task["last_event_hash"] != previous_hash:
            raise DomainError("EVENT_CHAIN_INVALID")

    def audit_task_events(self, task_id: str) -> bool:
        """Read-only event continuity check for an existing task."""
        with closing(self._connect()) as c:
            self._verify_event_chain(c, self._task(c, task_id))
        return True

    @staticmethod
    def _append(c: sqlite3.Connection, task: sqlite3.Row, kind: str, payload: dict) -> None:
        SqliteStore._verify_event_chain(c, task)
        seq = task["event_seq"] + 1
        event = {"task_id": task["task_id"], "seq": seq, "kind": kind, "payload": payload,
                 "previous_hash": task["last_event_hash"]}
        digest = content_hash(event)
        c.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)",
                  (uid(), task["task_id"], seq, kind, canonical_bytes(payload).decode("utf-8"),
                   task["last_event_hash"], digest, utc_now()))
        c.execute("UPDATE tasks SET event_seq=?,last_event_hash=? WHERE task_id=?",
                  (seq, digest, task["task_id"]))

    def _advance(self, c: sqlite3.Connection, task_id: str, expected: int,
                 new_status: Status, event: str, payload: dict | None = None) -> int:
        task = self._task(c, task_id)
        if task["state_version"] != expected:
            raise DomainError("STALE_STATE")
        allowed = {
            "CREATED": {"READY"},
            "READY": {"RUNNING", "FAILED"},
            "RUNNING": {"WAITING_EXECUTION", "VERIFYING", "FAILED"},
            "WAITING_EXECUTION": {"WAITING_EXECUTION", "VERIFYING", "HUMAN_REVIEW", "FAILED"},
            "VERIFYING": {"RUNNING", "SUCCEEDED", "FAILED", "HUMAN_REVIEW"},
            "RECOVERING": {"RUNNING", "FAILED", "HUMAN_REVIEW"},
        }
        if new_status.value not in allowed.get(task["status"], set()):
            raise DomainError("ILLEGAL_STATE_TRANSITION")
        changed = c.execute("UPDATE tasks SET state_version=state_version+1,status=? "
                            "WHERE task_id=? AND state_version=?",
                            (new_status.value, task_id, expected)).rowcount
        if changed != 1:
            raise DomainError("STALE_STATE")
        self._append(c, task, event, payload or {})
        return expected + 1

    def create_task(self, task: TaskContract) -> int:
        with self._write() as c:
            active = c.execute("SELECT release_id FROM active_release WHERE singleton=1").fetchone()
            manifest = c.execute("SELECT * FROM release_manifests WHERE release_id=?",
                                 (task.release_id,)).fetchone()
            pointer = c.execute(
                "SELECT release_id,manifest_hash,revision FROM release_pointer_state "
                "WHERE singleton=1"
            ).fetchone()
            if (active is None or active[0] != task.release_id or manifest is None
                or pointer is None or pointer["release_id"] != task.release_id
                or pointer["manifest_hash"] != manifest["body_hash"]
                or pointer["revision"] != 0
                or task.release_id != "R0-FAKE"
                or manifest["body"] != canonical_bytes(R0_MANIFEST).decode("utf-8")
                or manifest["body_hash"] != content_hash(R0_MANIFEST)):
                raise DomainError("INVALID_RELEASE_PIN")
            if not set(task.allowed_capabilities).issubset(set(cast(list[str], R0_MANIFEST["capabilities"]))):
                raise DomainError("CAPABILITY_DENIED")
            goal = canonical_bytes(asdict(task.goal)).decode("utf-8")
            c.execute("INSERT INTO tasks(task_id,release_id,release_hash,goal_json,status,state_version,"
                      "max_steps,max_retries,created_at) VALUES (?,?,?,?,?,0,?,?,?)",
                      (task.task_id, task.release_id, manifest["body_hash"], goal,
                       Status.CREATED.value, task.max_steps, task.max_retries, utc_now()))
            self._advance(c, task.task_id, 0, Status.READY, "TASK_ACCEPTED",
                          {"release_id": task.release_id})
            return 1

    def current(self, task_id: str) -> dict:
        with closing(self._connect()) as c:
            return dict(self._task(c, task_id))

    def transition(self, task_id: str, expected: int, new_status: Status,
                   event: str, payload: dict | None = None) -> int:
        if new_status not in {Status.RUNNING, Status.VERIFYING}:
            raise DomainError("PRIVILEGED_TRANSITION")
        with self._write() as c:
            return self._advance(c, task_id, expected, new_status, event, payload)

    @staticmethod
    def _ensure_resource_clear(
        c: sqlite3.Connection, resource: str, exclude_invocation: str | None = None,
    ) -> None:
        """Fail closed even when an earlier process crashed before recovery.

        Only one local Runtime worker is supported. This is a defensive DB guard,
        not distributed or physical device exclusivity.
        """
        quarantined = c.execute(
            "SELECT 1 FROM resource_quarantine WHERE resource=?", (resource,)
        ).fetchone()
        if quarantined is not None:
            raise DomainError("RESOURCE_QUARANTINED")
        pending = c.execute(
            "SELECT 1 FROM invocations AS i JOIN tasks AS t ON t.task_id=i.task_id "
            "WHERE i.resource=? AND i.invocation_id<>? "
            "AND (i.status IN ('INTENT_RECORDED','DISPATCH_STARTED') "
            "OR (i.status='PROVIDER_RETURNED' "
            "AND t.status NOT IN ('SUCCEEDED','FAILED','HUMAN_REVIEW'))) LIMIT 1",
            (resource, exclude_invocation or ""),
        ).fetchone()
        if pending is not None:
            raise DomainError("RESOURCE_IN_FLIGHT")

    def recover_inflight(self) -> list[dict[str, str]]:
        """Reconcile abandoned local tasks without ever invoking an adapter.

        Call this only during single-worker startup, when the previous owner is
        known to have exited. A persisted intent is conservatively ambiguous.
        Recovery events, task transitions and quarantine are in one transaction.
        Existing HUMAN_REVIEW records are left untouched (idempotent replay).
        """
        changes: list[dict[str, str]] = []
        with self._write() as c:
            tasks = c.execute(
                "SELECT task_id,status,state_version FROM tasks "
                "WHERE status IN ('RUNNING','WAITING_EXECUTION','VERIFYING') "
                "ORDER BY created_at,task_id"
            ).fetchall()
            for candidate in tasks:
                task_id = cast(str, candidate["task_id"])
                versions = cast(int, candidate["state_version"])
                status = cast(str, candidate["status"])
                invocations = c.execute(
                    "SELECT invocation_id,status,resource FROM invocations "
                    "WHERE task_id=?", (task_id,)
                ).fetchall()
                if len(invocations) > 1:
                    raise DomainError("UNSUPPORTED_RECOVERY_HISTORY")
                if not invocations:
                    if status != Status.RUNNING.value:
                        raise DomainError("CORRUPT_INFLIGHT_STATE")
                    self._advance(c, task_id, versions, Status.FAILED,
                                  "RECOVERY_NO_DISPATCH", {"reason": "INTERRUPTED_BEFORE_INTENT"})
                    changes.append({"task_id": task_id, "status": "FAILED",
                                    "reason": "INTERRUPTED_BEFORE_INTENT"})
                    continue
                inv = invocations[0]
                invocation_id = cast(str, inv["invocation_id"])
                previous = cast(str, inv["status"])
                resource = cast(str, inv["resource"])
                if previous not in {"INTENT_RECORDED", "DISPATCH_STARTED", "PROVIDER_RETURNED"}:
                    raise DomainError("UNRECOGNIZED_INFLIGHT_INVOCATION")
                # No Stop ACK or provider query can be inferred after a crash.
                c.execute(
                    "UPDATE invocations SET status='EXECUTION_UNKNOWN' "
                    "WHERE invocation_id=? AND status=?", (invocation_id, previous),
                )
                c.execute(
                    "INSERT OR IGNORE INTO resource_quarantine "
                    "(resource,invocation_id,reason,created_at) VALUES (?,?,?,?)",
                    (resource, invocation_id, "RESTART_UNCERTAIN", utc_now()),
                )
                self._advance(
                    c, task_id, versions, Status.HUMAN_REVIEW, "RECOVERY_UNKNOWN",
                    {"invocation_id": invocation_id, "previous_status": previous,
                     "reason": "RESTART_UNCERTAIN"},
                )
                changes.append({"task_id": task_id, "status": "HUMAN_REVIEW",
                                "reason": "RESTART_UNCERTAIN"})
        return changes

    def record_intent(self, ticket: AuthorizedInvocation, plan: PlanStep) -> int:
        with self._write() as c:
            task = self._task(c, ticket.task_id)
            if (task["state_version"] != ticket.state_version
                or task["release_id"] != ticket.release_id
                or plan.expected_state_version != ticket.state_version
                or task["status"] not in {Status.READY.value, Status.RUNNING.value}):
                raise DomainError("STALE_INTENT")
            owner = self._fence.get()
            if owner is not None and ticket.fencing_token != owner.token:
                raise DomainError("TICKET_FENCE_MISMATCH")
            self._ensure_resource_clear(c, ticket.resource)
            used = c.execute("SELECT COUNT(*) FROM invocations WHERE task_id=?", (ticket.task_id,)).fetchone()[0]
            if used >= task["max_steps"] + task["max_retries"]:
                raise DomainError("BUDGET_EXHAUSTED")
            from ...harness.skill_registry import SqliteSkillRegistry
            SqliteSkillRegistry(self).record_authorized_intent(c, ticket, plan)
            c.execute("INSERT INTO invocations VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      (ticket.invocation_id, ticket.task_id, ticket.step_id, ticket.release_id,
                       ticket.tool_id, ticket.arguments_hash,
                       canonical_bytes(asdict(plan.postcondition)).decode("utf-8"),
                       ticket.state_version, "INTENT_RECORDED", None, ticket.resource))
            return self._advance(c, ticket.task_id, ticket.state_version,
                                 Status.WAITING_EXECUTION, "INVOCATION_INTENT",
                                 {"invocation_id": ticket.invocation_id, "tool_id": ticket.tool_id})

    def mark_dispatched(self, task_id: str, invocation_id: str, expected: int) -> int:
        with self._write() as c:
            inv = c.execute("SELECT * FROM invocations WHERE invocation_id=? AND task_id=?",
                            (invocation_id, task_id)).fetchone()
            if inv is None or inv["status"] != "INTENT_RECORDED":
                raise DomainError("INVALID_DISPATCH")
            self._ensure_resource_clear(c, cast(str, inv["resource"]), invocation_id)
            task = self._task(c, task_id)
            if task["state_version"] != expected:
                raise DomainError("STALE_STATE")
            c.execute("UPDATE invocations SET status='DISPATCH_STARTED' WHERE invocation_id=?",
                      (invocation_id,))
            return self._advance(c, task_id, expected, Status.WAITING_EXECUTION,
                                 "DISPATCH_STARTED", {"invocation_id": invocation_id})

    def record_response(self, task_id: str, response: ProviderResponse, expected: int) -> int:
        with self._write() as c:
            inv = c.execute("SELECT * FROM invocations WHERE invocation_id=? AND task_id=?",
                            (response.invocation_id, task_id)).fetchone()
            if inv is None or inv["status"] != "DISPATCH_STARTED":
                raise DomainError("INVALID_PROVIDER_RESPONSE")
            if response.status != "COMPLETED":
                raise DomainError("PROVIDER_NOT_COMPLETE")
            c.execute("UPDATE invocations SET status='PROVIDER_RETURNED',provider_status=? "
                      "WHERE invocation_id=?", (response.status, response.invocation_id))
            return self._advance(c, task_id, expected, Status.VERIFYING,
                                 "PROVIDER_RETURNED", {"invocation_id": response.invocation_id,
                                                       "provider_status": response.status})

    def quarantine_unverifiable_execution(
        self, task_id: str, invocation_id: str, expected: int, reason: str,
    ) -> int:
        """Atomically quarantine a completed invocation with unverified effects.

        A provider's COMPLETED response is not evidence of the environment's
        state. If post-dispatch observation is unavailable/inconclusive, do
        not mistake task failure for proof that the resource is safe to reuse.
        No physical stop or re-execution is attempted by this method.
        """
        if reason != "POST_OBSERVATION_ERROR":
            raise DomainError("INVALID_UNCERTAINTY_REASON")
        with self._write() as c:
            task = self._task(c, task_id)
            inv = c.execute(
                "SELECT * FROM invocations WHERE invocation_id=? AND task_id=?",
                (invocation_id, task_id),
            ).fetchone()
            if (inv is None or inv["status"] != "PROVIDER_RETURNED"
                or task["status"] != Status.VERIFYING.value):
                raise DomainError("INVALID_UNCERTAIN_EVIDENCE_STATE")
            if task["state_version"] != expected:
                raise DomainError("STALE_STATE")
            resource = cast(str, inv["resource"])
            c.execute(
                "INSERT INTO resource_quarantine "
                "(resource,invocation_id,reason,created_at) VALUES (?,?,?,?)",
                (resource, invocation_id, reason, utc_now()),
            )
            c.execute(
                "UPDATE invocations SET status='EXECUTION_UNKNOWN' WHERE invocation_id=?",
                (invocation_id,),
            )
            return self._advance(
                c, task_id, expected, Status.HUMAN_REVIEW, "EXECUTION_UNKNOWN",
                {"invocation_id": invocation_id, "reason": reason,
                 "external_stop_confirmed": False},
            )

    def save_evidence(self, evidence: EvidenceBundle, verdict: VerificationVerdict) -> None:
        if (evidence.task_id != verdict.task_id or evidence.step_id != verdict.step_id
            or evidence.scope != verdict.scope or evidence.release_id != verdict.release_id
            or evidence_digest(evidence) != verdict.evidence_hash):
            raise DomainError("EVIDENCE_MISMATCH")
        with self._write() as c:
            task = self._task(c, evidence.task_id)
            inv = c.execute("SELECT * FROM invocations WHERE invocation_id=? AND task_id=?",
                            (evidence.invocation_id, evidence.task_id)).fetchone()
            if (task["status"] != Status.VERIFYING.value or inv is None
                or inv["status"] != "PROVIDER_RETURNED"):
                raise DomainError("EVIDENCE_WRONG_STATE")
            c.execute("INSERT INTO evidences VALUES (?,?,?,?,?,?)",
                      (evidence.evidence_id, evidence.task_id, evidence.invocation_id,
                       evidence.scope, canonical_bytes(asdict(evidence)).decode("utf-8"),
                       evidence_digest(evidence)))
            c.execute("INSERT INTO verdicts VALUES (?,?,?,?,?)",
                      (verdict.verdict_id, evidence.evidence_id, evidence.task_id,
                       canonical_bytes(asdict(verdict)).decode("utf-8"),
                       content_hash(asdict(verdict))))
            self._append(c, task, "EVIDENCE_CAPTURED",
                         {"scope": evidence.scope, "evidence_id": evidence.evidence_id,
                          "verdict_id": verdict.verdict_id})

    def commit(self, task_id: str, evidence: EvidenceBundle, verdict: VerificationVerdict,
               expected_state_version: int, idempotency_key: str) -> str:
        request = {"task_id": task_id, "scope": evidence.scope, "step_id": evidence.step_id,
                   "evidence_id": evidence.evidence_id, "verdict_id": verdict.verdict_id,
                   "expected_state_version": expected_state_version}
        req_hash = content_hash(request)
        with self._write() as c:
            old = c.execute("SELECT request_hash FROM commit_records WHERE task_id=? AND idempotency_key=?",
                            (task_id, idempotency_key)).fetchone()
            if old is not None:
                if old["request_hash"] == req_hash:
                    return "ALREADY_COMMITTED"
                raise DomainError("IDEMPOTENCY_CONFLICT")
            task = self._task(c, task_id)
            if task["state_version"] != expected_state_version:
                raise DomainError("STALE_STATE")
            if task["status"] != Status.VERIFYING.value:
                raise DomainError("INVALID_COMMIT_STATE")
            inv = c.execute("SELECT * FROM invocations WHERE invocation_id=? AND task_id=?",
                            (evidence.invocation_id, task_id)).fetchone()
            if inv is None or inv["status"] != "PROVIDER_RETURNED":
                raise DomainError("UNVERIFIED_INVOCATION")
            if task["release_id"] != evidence.release_id or inv["release_id"] != evidence.release_id:
                raise DomainError("RELEASE_MISMATCH")
            e = c.execute("SELECT * FROM evidences WHERE evidence_id=? AND task_id=?",
                          (evidence.evidence_id, task_id)).fetchone()
            v = c.execute("SELECT * FROM verdicts WHERE verdict_id=? AND evidence_id=?",
                          (verdict.verdict_id, evidence.evidence_id)).fetchone()
            if (e is None or v is None or e["payload"] != canonical_bytes(asdict(evidence)).decode("utf-8")
                or e["evidence_hash"] != evidence_digest(evidence)
                or v["payload"] != canonical_bytes(asdict(verdict)).decode("utf-8")
                or v["verdict_hash"] != content_hash(asdict(verdict))):
                raise DomainError("EVIDENCE_MISMATCH")
            if verdict.result != Verdict.SUCCESS or verdict.verifier_id != IndependentVerifier.ID:
                raise DomainError("VERIFICATION_NOT_SUCCESS")
            if evidence.scope == "STEP":
                cond = json.loads(inv["step_condition_json"])
                new_status = Status.RUNNING
                if evidence.step_id != inv["step_id"]:
                    raise DomainError("STEP_MISMATCH")
            elif evidence.scope == "GOAL":
                cond = json.loads(task["goal_json"])
                new_status = Status.SUCCEEDED
                if evidence.step_id is not None:
                    raise DomainError("GOAL_SCOPE_MISMATCH")
                has_step = c.execute("SELECT 1 FROM commit_records WHERE task_id=? AND scope='STEP'",
                                     (task_id,)).fetchone()
                if has_step is None:
                    raise DomainError("STEP_NOT_COMMITTED")
            else:
                raise DomainError("INVALID_SCOPE")
            from ...core.contracts import Postcondition
            checked = IndependentVerifier().verify(evidence, Postcondition(**cond))
            if (checked.result != Verdict.SUCCESS or verdict.condition_id != cond["condition_id"]
                or verdict.scope != evidence.scope or verdict.step_id != evidence.step_id):
                raise DomainError("VERIFICATION_NOT_SUCCESS")
            new_ver = self._advance(c, task_id, expected_state_version,
                                    new_status, "COMMIT_ACCEPTED",
                                    {"scope": evidence.scope, "evidence_id": evidence.evidence_id})
            commit_id = uid()
            c.execute("INSERT INTO commit_records VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      (commit_id, task_id, evidence.scope, evidence.step_id, evidence.evidence_id,
                       verdict.verdict_id, idempotency_key, req_hash,
                       expected_state_version, new_ver, utc_now()))
            if evidence.scope == "GOAL":
                # Durable projection intent is atomic with GOAL CommitRecord.
                # A failed Memory projector never invalidates the successful commit.
                c.execute(
                    "INSERT INTO outbox VALUES (?,?,?,?,?,0)",
                    (uid(), task_id, "MEMORY_VERIFIED_PENDING",
                     canonical_bytes({"commit_id": commit_id, "task_id": task_id,
                                      "release_id": task["release_id"]}).decode("utf-8"),
                     utc_now()),
                )
                c.execute("INSERT INTO outbox VALUES (?,?,?,?,?,0)",
                          (uid(), task_id, "GOAL_COMMITTED",
                           canonical_bytes({"task_id": task_id, "release_id": task["release_id"]}).decode("utf-8"), utc_now()))
            return "COMMITTED"

    def request_cancel(self, task_id: str) -> bool:
        """Durably record an advisory request; never assert external stop."""
        # Cancellation is an external advisory input, intentionally allowed
        # from a separate thread/process; it cannot authorize an action.
        with self._write(external_cancel=True) as c:
            task = self._task(c, task_id)
            if task["status"] in {Status.SUCCEEDED.value, Status.FAILED.value,
                                  Status.HUMAN_REVIEW.value}:
                raise DomainError("ALREADY_TERMINAL")
            inserted = c.execute(
                "INSERT OR IGNORE INTO task_cancellations VALUES (?,?)",
                (task_id, utc_now()),
            ).rowcount
            if inserted:
                self._append(c, task, "TASK_CANCEL_REQUESTED", {})
            return bool(inserted)

    def is_cancel_requested(self, task_id: str) -> bool:
        with closing(self._connect()) as c:
            return c.execute(
                "SELECT 1 FROM task_cancellations WHERE task_id=?", (task_id,)
            ).fetchone() is not None

    def request_stop(self, task_id: str, invocation_id: str, expected: int,
                     reason: str) -> None:
        """Persist STOP_REQUESTED *before* calling the external stop endpoint."""
        if reason not in {"TIMEOUT", "CANCELLED", "ADAPTER_EXCEPTION"}:
            raise DomainError("INVALID_STOP_REASON")
        with self._write() as c:
            task = self._task(c, task_id)
            inv = c.execute(
                "SELECT 1 FROM invocations WHERE invocation_id=? AND task_id=? "
                "AND status='DISPATCH_STARTED'", (invocation_id, task_id)
            ).fetchone()
            if inv is None or task["state_version"] != expected:
                raise DomainError("INVALID_STOP_STATE")
            inserted = c.execute(
                "INSERT OR IGNORE INTO stop_requests "
                "(invocation_id,task_id,reason,requested_at) VALUES (?,?,?,?)",
                (invocation_id, task_id, reason, utc_now()),
            ).rowcount
            if inserted != 1:
                raise DomainError("STOP_ALREADY_REQUESTED")
            self._append(c, task, "STOP_REQUESTED", {"invocation_id": invocation_id,
                                                     "reason": reason})

    def mark_unknown(self, task_id: str, invocation_id: str, expected: int,
                     resource: str, stop_ack: str) -> int:
        """Atomically journal stop acknowledgement, quarantine and HUMAN_REVIEW.

        Even CONFIRMED is advisory: the resource requires external reconciliation
        before being released. This prototype supplies no release method.
        """
        if stop_ack not in {"CONFIRMED", "REJECTED", "UNKNOWN"}:
            raise DomainError("INVALID_STOP_ACK")
        with self._write() as c:
            inv = c.execute("SELECT * FROM invocations WHERE invocation_id=? AND task_id=?",
                            (invocation_id, task_id)).fetchone()
            stop = c.execute(
                "SELECT * FROM stop_requests WHERE invocation_id=? AND task_id=?",
                (invocation_id, task_id),
            ).fetchone()
            if (inv is None or inv["status"] != "DISPATCH_STARTED"
                or inv["resource"] != resource or stop is None
                or stop["acknowledgement"] != "PENDING"):
                raise DomainError("INVALID_STOP")
            task = self._task(c, task_id)
            if task["state_version"] != expected:
                raise DomainError("STALE_STATE")
            c.execute("UPDATE stop_requests SET acknowledgement=?,acknowledged_at=? "
                      "WHERE invocation_id=?", (stop_ack, utc_now(), invocation_id))
            c.execute("UPDATE invocations SET status='EXECUTION_UNKNOWN' WHERE invocation_id=?",
                      (invocation_id,))
            c.execute("INSERT INTO resource_quarantine VALUES (?,?,?,?)",
                      (resource, invocation_id, "STOP_" + stop_ack, utc_now()))
            self._append(c, task, "STOP_ACK_RECORDED",
                         {"invocation_id": invocation_id, "ack": stop_ack})
            return self._advance(c, task_id, expected, Status.HUMAN_REVIEW,
                                 "EXECUTION_UNKNOWN", {"invocation_id": invocation_id,
                                                       "stop_ack": stop_ack})

    def stop_record(self, invocation_id: str) -> dict | None:
        with closing(self._connect()) as c:
            row = c.execute("SELECT * FROM stop_requests WHERE invocation_id=?",
                            (invocation_id,)).fetchone()
            return dict(row) if row is not None else None

    def fail_task(self, task_id: str, expected: int, code: str) -> int:
        with self._write() as c:
            row = self._task(c, task_id)
            if row["status"] in {Status.SUCCEEDED.value, Status.HUMAN_REVIEW.value, Status.FAILED.value}:
                raise DomainError("ALREADY_TERMINAL")
            # A failed *verification* is not proof that an external side effect
            # did not occur. Preserve the existing FAILED task/oracle semantics
            # while quarantining the resource until external reconciliation.
            if code in {"STEP_INCONCLUSIVE", "GOAL_INCONCLUSIVE"}:
                inv = c.execute(
                    "SELECT invocation_id,resource FROM invocations "
                    "WHERE task_id=? AND status='PROVIDER_RETURNED'",
                    (task_id,),
                ).fetchone()
                if inv is None:
                    raise DomainError("INCONCLUSIVE_WITHOUT_DISPATCHED_EFFECT")
                c.execute(
                    "INSERT INTO resource_quarantine "
                    "(resource,invocation_id,reason,created_at) VALUES (?,?,?,?)",
                    (inv["resource"], inv["invocation_id"], "VERIFICATION_INCONCLUSIVE", utc_now()),
                )
            return self._advance(c, task_id, expected, Status.FAILED,
                                 "TASK_FAILED", {"code": code})

    def outcome(self, task_id: str, failure_code: str | None = None) -> TaskOutcome:
        with closing(self._connect()) as c:
            row = self._task(c, task_id)
            count = c.execute("SELECT COUNT(*) FROM invocations WHERE task_id=?", (task_id,)).fetchone()[0]
            return TaskOutcome(task_id, Status(row["status"]), row["release_id"],
                               row["state_version"], count, failure_code)

    def event_kinds(self, task_id: str) -> list[str]:
        with closing(self._connect()) as c:
            return [r[0] for r in c.execute("SELECT kind FROM events WHERE task_id=? ORDER BY seq", (task_id,))]

    def table_count(self, table: str) -> int:
        if table not in {"tasks", "events", "invocations", "commit_records", "resource_quarantine", "outbox", "stop_requests", "task_cancellations", "memory_records", "memory_revocations", "memory_audit", "skill_definitions", "skill_states", "release_skill_bindings", "skill_audit", "experience_records", "diagnosis_records", "evolution_candidates", "candidate_decisions", "evaluation_suites", "candidate_evaluations", "candidate_review_requests", "candidate_review_decisions", "release_pointer_state", "staged_release_manifests", "release_activation_attempts", "release_rollback_attempts", "release_manifests", "active_release"}:
            raise ValueError("unapproved table")
        with closing(self._connect()) as c:
            return cast(int, c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def connection_settings(self) -> dict[str, int | str]:
        with closing(self._connect()) as c:
            return {"journal_mode": c.execute("PRAGMA journal_mode").fetchone()[0],
                    "synchronous": c.execute("PRAGMA synchronous").fetchone()[0],
                    "foreign_keys": c.execute("PRAGMA foreign_keys").fetchone()[0],
                    "integrity": c.execute("PRAGMA integrity_check").fetchone()[0]}
