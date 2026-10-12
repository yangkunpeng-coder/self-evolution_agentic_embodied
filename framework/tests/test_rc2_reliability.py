"""RC2 infrastructure acceptance on synthetic tasks only (no model or robot)."""
from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
import time
from pathlib import Path

import pytest

from self_evolving_embodied.core.contracts import DomainError
from self_evolving_embodied.planning.validator import PlanValidator
from self_evolving_embodied.runtime.workflow_orchestrator import WorkflowOrchestrator
from self_evolving_embodied.runtime.workflow_store import WorkflowStore, _digest
from self_evolving_embodied.storage.sqlite.database import SqliteStore
from self_evolving_embodied.tools.contracts import Effect, ToolKind, ToolSpec
from self_evolving_embodied.tools.registry import PluggableToolRegistry

LEGACY_SEED = Path(__file__).with_name('legacy_v1_0_0_seed.sqlite3.gz')


def _legacy_copy(destination: Path) -> None:
    # Generated using the archived public v1.0.0 SqliteStore; no personal data.
    with gzip.open(LEGACY_SEED, 'rb') as src, destination.open('wb') as dst:
        shutil.copyfileobj(src, dst)


def _version(path: Path) -> int:
    with sqlite3.connect(path) as db:
        return db.execute('SELECT version FROM schema_meta').fetchone()[0]


def _registry(*, effectful: bool = False):
    registry = PluggableToolRegistry()
    spec = ToolSpec(
        'control.approach' if effectful else 'perception.describe',
        'control.approach' if effectful else 'perception.describe',
        ToolKind.CONTROLLER if effectful else ToolKind.VLM,
        'Synthetic RC2 tool', (('query', 'string'),),
        ('result_available',),
        effect=Effect.PHYSICAL_ACTION if effectful else Effect.READ_ONLY,
    )
    registry.register(spec)
    return registry, spec


def _plan(registry, spec):
    return PlanValidator(registry).parse(json.dumps({
        'goal': 'Inspect object',
        'subtasks': [{
            'id': 'inspect', 'tool_id': spec.tool_id,
            'arguments': {'query': 'object'},
            'depends_on': [], 'success_condition': 'result_available',
        }],
    }), 'Inspect object')


def test_migrate_actual_v1_0_0_database_to_v18_preserves_data(tmp_path):
    path = tmp_path / 'historical.sqlite3'
    _legacy_copy(path)
    assert _version(path) == 12
    with sqlite3.connect(path) as db:
        original = db.execute('SELECT body_hash FROM release_manifests WHERE release_id=?',
                              ('R0-FAKE',)).fetchone()[0]
    store = SqliteStore(path)
    assert _version(path) == 18
    with store._connect() as db:
        assert db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
        assert db.execute('PRAGMA foreign_key_check').fetchone() is None
        assert db.execute('SELECT body_hash FROM release_manifests WHERE release_id=?',
                          ('R0-FAKE',)).fetchone()[0] == original
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='agent_workflows'").fetchone()
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='agent_runtime_routing_pins'").fetchone()
    SqliteStore(path)  # repeated open is idempotent
    assert _version(path) == 18


def test_future_schema_rejected_without_downgrade(tmp_path):
    path = tmp_path / 'future.sqlite3'
    _legacy_copy(path)
    with sqlite3.connect(path) as db:
        db.execute('UPDATE schema_meta SET version=999')
    with pytest.raises(DomainError, match='UNSUPPORTED_SCHEMA'):
        SqliteStore(path)
    assert _version(path) == 999


def test_legacy_missing_schema_metadata_rejected_without_bootstrap(tmp_path):
    path = tmp_path / 'missing_metadata.sqlite3'
    _legacy_copy(path)
    with sqlite3.connect(path) as db:
        db.execute('DROP TABLE schema_meta')
    with pytest.raises(DomainError, match='SCHEMA_METADATA_MISSING'):
        SqliteStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT COUNT(*) FROM release_manifests').fetchone()[0] >= 1
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='agent_workflows'").fetchone() is None



def test_migration_rejects_shadow_core_table_atomically(tmp_path):
    path = tmp_path / 'conflicting_schema.sqlite3'
    _legacy_copy(path)
    with sqlite3.connect(path) as db:
        # A pre-existing incomplete table must not be silently accepted by
        # CREATE TABLE IF NOT EXISTS during a version upgrade.
        db.execute('CREATE TABLE agent_workflows (run_id TEXT PRIMARY KEY)')
    with pytest.raises(DomainError, match='INCOMPATIBLE_SCHEMA_LAYOUT'):
        SqliteStore(path)
    assert _version(path) == 12
    with sqlite3.connect(path) as db:
        assert db.execute('PRAGMA table_info(agent_workflows)').fetchall() == [
            (0, 'run_id', 'TEXT', 0, None, 1),
        ]
        # The entire upgrade transaction must roll back, not half-initialize.
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='agent_subtasks'").fetchone() is None


def test_worker_lease_fences_stale_context(tmp_path, monkeypatch):
    store = SqliteStore(tmp_path / 'lease.sqlite3')
    current = [1_000_000]
    monkeypatch.setattr(store, '_now_ms', lambda: current[0])
    with store.worker_session(owner_id='first', lease_ms=500):
        first_token = store.active_fencing_token()
        store.heartbeat_worker()
        current[0] += 501
        with pytest.raises(DomainError, match='STALE_WORKER_FENCE'):
            store.heartbeat_worker()
        with store.worker_session(owner_id='replacement', lease_ms=500):
            assert store.active_fencing_token() > first_token
        with pytest.raises(DomainError, match='STALE_WORKER_FENCE'):
            store.active_fencing_token()
    assert store.lease_info()['owner_id'] is None


def test_unfinished_read_only_intent_requires_review_without_replay(tmp_path):
    store = SqliteStore(tmp_path / 'read_only_recovery.sqlite3')
    registry, spec = _registry()
    plan = _plan(registry, spec)
    orchestrator = WorkflowOrchestrator(store, registry, {
        'result_available': lambda _args, _res: True,
    })
    with store.worker_session():
        orchestrator.workflows.create('read-only-run', plan, orchestrator.catalog_hash())
        orchestrator.workflows.begin('read-only-run', 'inspect', 'invocation-1')
    calls = []

    class Handler:
        def invoke(self, arguments):
            calls.append(arguments)
            return {'result_available': True}

    registry.bind(spec.tool_id, Handler())
    result = orchestrator.resume('read-only-run')
    assert result.status == 'HUMAN_REVIEW'
    assert result.subtask_states['inspect'] == 'HUMAN_REVIEW'
    assert calls == []  # never duplicate a persisted uncertain call
    with store._connect() as db:
        events = [r[0] for r in db.execute(
            'SELECT kind FROM agent_workflow_events WHERE run_id=? ORDER BY seq',
            ('read-only-run',))]
    assert 'WORKFLOW_QUARANTINED' in events
    assert orchestrator.resume('read-only-run').status == 'HUMAN_REVIEW'
    assert calls == []


def test_unknown_effectful_intent_quarantines_resource_and_blocks_reuse(tmp_path):
    store = SqliteStore(tmp_path / 'effectful_recovery.sqlite3')
    registry, spec = _registry(effectful=True)
    plan = _plan(registry, spec)
    workflow = WorkflowStore(store)
    with store.worker_session():
        workflow.create('first', plan, 'fixed-catalog')
        workflow.begin('first', 'inspect', 'physical-invocation', {
            'tool_id': spec.tool_id,
            'resource': 'test-robot-arm',
            'arguments_hash': _digest({'query': 'object'}),
            'fencing_token': store.active_fencing_token(),
        })
    # Simulate host restart by replacing the SqliteStore instance.
    recovered = SqliteStore(store.path)
    recovered_workflow = WorkflowStore(recovered)
    with recovered.worker_session():
        assert recovered_workflow.recover_inflight('first') is True
        assert recovered_workflow.snapshot('first').status == 'HUMAN_REVIEW'
        recovered_workflow.create('second', plan, 'fixed-catalog')
        with pytest.raises(DomainError, match='RESOURCE_QUARANTINED|RESOURCE_IN_FLIGHT'):
            recovered_workflow.begin('second', 'inspect', 'another-invocation', {
                'tool_id': spec.tool_id,
                'resource': 'test-robot-arm',
                'arguments_hash': _digest({'query': 'object'}),
                'fencing_token': recovered.active_fencing_token(),
            })
    with recovered._connect() as db:
        assert db.execute('SELECT status FROM agent_effectful_invocations WHERE invocation_id=?',
                          ('physical-invocation',)).fetchone()[0] == 'QUARANTINED'
        assert db.execute('SELECT reason FROM resource_quarantine WHERE resource=?',
                          ('test-robot-arm',)).fetchone()[0] == 'SUBTASK_OUTCOME_UNKNOWN'


def test_event_chain_modification_detected_before_resume(tmp_path):
    store = SqliteStore(tmp_path / 'altered.sqlite3')
    registry, spec = _registry()
    workflow = WorkflowStore(store)
    with store.worker_session():
        workflow.create('event-tampering', _plan(registry, spec), 'fixed-catalog')
    with sqlite3.connect(store.path) as db:
        with pytest.raises(sqlite3.IntegrityError, match='immutable'):
            db.execute('UPDATE agent_workflow_events SET payload_json=? WHERE run_id=? AND seq=1',
                       ('{"unexpected":"edit"}', 'event-tampering'))
        # Tamper simulation with arbitrary database-write rights: local hash
        # chains detect accidental edits, not malicious fully-rehashed history.
        db.execute('DROP TRIGGER immutable_agent_workflow_event_update')
        db.execute('UPDATE agent_workflow_events SET payload_json=? WHERE run_id=? AND seq=1',
                   ('{"unexpected":"edit"}', 'event-tampering'))
    with pytest.raises(DomainError, match='WORKFLOW_EVENT_CORRUPTED'):
        workflow.snapshot('event-tampering')


def test_read_only_result_returning_after_deadline_is_rejected(tmp_path):
    store = SqliteStore(tmp_path / 'provider_deadline.sqlite3')
    registry, spec = _registry()

    class SlowProvider:
        def invoke(self, _arguments):
            time.sleep(0.04)
            return {'result_available': True}

    registry.bind(spec.tool_id, SlowProvider())
    orchestrator = WorkflowOrchestrator(store, registry)
    with store.worker_session():
        with pytest.raises(DomainError, match='READ_ONLY_TOOL_TIMEOUT'):
            orchestrator._invoke_read_only_bounded(
                spec.tool_id, {'query': 'object'}, timeout_seconds=0.005)
