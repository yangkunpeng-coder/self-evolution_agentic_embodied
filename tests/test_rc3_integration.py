"""Windows/Python 3.12 RC3 regressions; all providers/devices are local stubs."""
from __future__ import annotations

from contextlib import closing
from dataclasses import replace
import json
import sqlite3
import sys
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from self_evolving_embodied import (
    AssessmentRoute, DomainError, FrameworkBuilder, FrameworkConfig, FrameworkState,
    ObservationInput, ObservationKey, ObservationRequest, SensorStreamConfig,
    build_configured_framework,
)
from self_evolving_embodied.agent import AgentAction, AgentDecision
from self_evolving_embodied.adapters.llm import OpenAICompatibleConfig, OpenAICompatibleProvider
from self_evolving_embodied.harness.execution_bridge import ExecutionGrant, ExecutionTicket
from self_evolving_embodied.integrations.ros2 import ROS2FencedActionAdapter, ROS2TopicBinding, ROS2TopicIngress
from self_evolving_embodied.observations import (
    PipelineConfig, QueueObservationPipeline, ScopedObservationReader,
    SynchronizationStatus, pipeline_from_toml,
)
from self_evolving_embodied.planning.validator import PlanValidator
from self_evolving_embodied.plugins import (
    FrameworkSettings, PluginKind, PluginManifest, PluginSelection, TrustedPluginCatalog,
)
from self_evolving_embodied.runtime.workflow_store import WorkflowStore
from self_evolving_embodied.storage.sqlite.database import SqliteStore
from self_evolving_embodied.toolboxes.assessment import (
    AssessmentRequest, AssessmentResult, AssessmentToolbox, AssessorSpec, Outcome,
)
from test_rc_acceptance import (
    ASSESS_SPEC, Brain, KEY, NEEDS, SPEC, Source, Tool, builder,
)


def _plan(registry, goal, count=1):
    return PlanValidator(registry).parse(json.dumps({
        'goal': goal, 'subtasks': [
            {'id': f's{i}', 'tool_id': SPEC.tool_id, 'arguments': {'query': goal},
             'depends_on': [f's{i-1}'] if i else [],
             'success_condition': 'tool_report_available'} for i in range(count)
        ]}), goal)


def _plain_framework(path, brain_factory, handler=None):
    class LocalTool:
        def invoke(self, arguments):
            return {'tool_report_available': True}
    b = FrameworkBuilder(FrameworkConfig(path), brain_factory=brain_factory)
    b.add_execution_tool(SPEC, factory=lambda _r: handler or LocalTool())
    b.add_evidence_policy('tool_report_available', lambda _a, out: out.get('tool_report_available') is True)
    return b.build()


def test_configured_plugins_execute_and_feed_assessment(tmp_path):
    events = []
    catalog = TrustedPluginCatalog()
    catalog.register_brain_factory(PluginManifest('host.brain', '1.0.0', PluginKind.BRAIN_FACTORY, 'Local brain'), Brain)
    catalog.register_execution(PluginManifest('host.tool', SPEC.version, PluginKind.EXECUTION, 'Tool'),
                               SPEC, needs=NEEDS, factory=Tool)
    class Assessor:
        def assess(self, request, reader):
            assert reader.latest(ObservationRequest(KEY)) is not None
            return AssessmentResult(Outcome.SUPPORTED, ASSESS_SPEC.assessor_id, 'Synthetic observation supports goal')
    catalog.register_assessor(PluginManifest('host.assessor', ASSESS_SPEC.version, PluginKind.ASSESSMENT, 'Assessor'),
                              ASSESS_SPEC, factory=Assessor)
    catalog.register_observation_source(PluginManifest('host.sensor', '1.0.0', PluginKind.OBSERVATION_SOURCE, 'Sensor'),
                                        lambda sink: Source(sink, events))
    settings = FrameworkSettings(FrameworkConfig(tmp_path/'configured.db'), (SensorStreamConfig(KEY, 30),),
        PluginSelection('host.brain', '1.0.0'), (PluginSelection('host.tool', SPEC.version),),
        (PluginSelection('host.assessor', ASSESS_SPEC.version),), (PluginSelection('host.sensor', '1.0.0'),))
    with build_configured_framework(settings, catalog,
            evidence_policies={'tool_report_available': lambda _a, out: out.get('tool_report_available') is True},
            assessment_routes=(AssessmentRoute(SPEC.tool_id, ASSESS_SPEC.assessor_id),)) as app:
        report = app.run_instruction('Inspect synthetic object')
        assert report.status == 'EXECUTION_WORKFLOW_COMPLETED'
        assert report.last_outcome == 'SUPPORTED'
    assert events == ['source_start', 'source_close']


def test_plugin_version_mismatch_precedes_factories_and_database(tmp_path):
    calls = []
    catalog = TrustedPluginCatalog()
    catalog.register_brain_factory(PluginManifest('host.brain', '1.0.0', PluginKind.BRAIN_FACTORY, 'Brain'),
                                   lambda _r: calls.append('brain'))
    settings = FrameworkSettings(FrameworkConfig(tmp_path/'absent.db'), (), PluginSelection('host.brain', '2.0.0'))
    with pytest.raises(DomainError, match='PLUGIN_VERSION_MISMATCH'):
        build_configured_framework(settings, catalog)
    assert calls == [] and not (tmp_path/'absent.db').exists()


@pytest.mark.parametrize('action,status', [
    (AgentAction.CONTINUE, 'EXECUTION_WORKFLOW_COMPLETED'),
    (AgentAction.STOP, 'HALTED'),
    (AgentAction.WAIT_FOR_OBSERVATION, 'WAITING_OBSERVATION'),
])
def test_journaled_decision_recovered_without_tool_replay(tmp_path, action, status):
    calls = []
    class LocalBrain(Brain):
        def plan(self, goal):
            return _plan(self.validator.registry, goal)
        def decide(self, context):
            return AgentDecision(action, 'Deterministic local decision')
    class CountingTool:
        def invoke(self, arguments):
            calls.append(arguments)
            return {'tool_report_available': True}
    path = tmp_path/'restart.db'
    with _plain_framework(path, LocalBrain, CountingTool()) as first:
        result = first.run_instruction('Inspect')
        assert result.status == status
    with _plain_framework(path, LocalBrain, CountingTool()) as restarted:
        assert restarted.resume_instruction(result.root_run_id).status == status
    assert len(calls) == 1


def test_replan_then_restart_follows_successor(tmp_path):
    decisions = []
    class LocalBrain(Brain):
        def decide(self, context):
            decisions.append(context.plan_revision)
            return AgentDecision(AgentAction.REPLAN if context.plan_revision == 0 else AgentAction.CONTINUE,
                                 'Deterministic revision decision')
    path = tmp_path/'replan.db'
    with _plain_framework(path, LocalBrain) as first:
        result = first.run_instruction('Inspect')
        assert result.replans == 1 and result.status == 'EXECUTION_WORKFLOW_COMPLETED'
        assert result.active_run_id != result.root_run_id
    with _plain_framework(path, LocalBrain) as restarted:
        resumed = restarted.resume_instruction(result.root_run_id)
        assert resumed.active_run_id == result.active_run_id and resumed.replans == 1
    assert decisions == [0, 1]


def test_second_read_only_failure_reaches_brain_replan(tmp_path):
    calls, revisions = [], []
    class LocalBrain(Brain):
        def plan(self, goal):
            return _plan(self.validator.registry, goal, 2)
        def replan(self, goal, context):
            revisions.append(context.execution_status)
            return _plan(self.validator.registry, goal)
        def decide(self, context):
            return AgentDecision(AgentAction.CONTINUE, 'Continue a verified read-only result')
    class FailSecond:
        def invoke(self, args):
            calls.append(args)
            if len(calls) == 2:
                raise RuntimeError('known local read-only failure')
            return {'tool_report_available': True}
    with _plain_framework(tmp_path/'second.db', LocalBrain, FailSecond()) as app:
        result = app.run_instruction('Inspect twice')
        assert result.status == 'EXECUTION_WORKFLOW_COMPLETED' and result.replans == 1
    assert len(calls) == 3 and revisions == ['BLOCKED']


def test_route_change_on_restart_is_rejected_before_new_action(tmp_path):
    first = builder(tmp_path, []).build()
    with first:
        first.publish_observation(ObservationInput(KEY, {'target_visible': False}))
        result = first.run_instruction('Inspect')
    changed = builder(tmp_path, [])
    other = replace(ASSESS_SPEC, assessor_id='assessment.changed')
    changed.add_assessor(other)
    # Same tool contracts, different assessor routing at host restart.
    changed._routes = [AssessmentRoute(SPEC.tool_id, other.assessor_id)]
    with changed.build() as restarted:
        with pytest.raises(DomainError, match='RUNTIME_ROUTING_CONFIGURATION_CHANGED'):
            restarted.resume_instruction(result.root_run_id, retry_observation=True)


def test_live_assessor_catalog_change_is_detected(tmp_path):
    with builder(tmp_path, []).build() as app:
        app.publish_observation(ObservationInput(KEY, {'target_visible': False}))
        result = app.run_instruction('Inspect')
        app.components.assessment.register(replace(ASSESS_SPEC, assessor_id='assessment.extra'))
        with pytest.raises(DomainError, match='RUNTIME_ROUTING_CONFIGURATION_CHANGED'):
            app.resume_instruction(result.root_run_id, retry_observation=True)


def test_workflow_read_connections_are_closed(tmp_path, monkeypatch):
    store = SqliteStore(tmp_path/'connections.db')
    opened = []
    connect = store._connect
    def tracked():
        connection = connect()
        opened.append(connection)  # retain references to prove deterministic closing
        return connection
    monkeypatch.setattr(store, '_connect', tracked)
    class LocalBrain(Brain):
        def decide(self, context):
            return AgentDecision(AgentAction.CONTINUE, 'Continue')
    with _plain_framework(tmp_path/'source.db', LocalBrain) as app:
        plan = _plan(app.components.registry, 'Inspect')
    workflows = WorkflowStore(store)
    with store.worker_session():
        workflows.create('reads', plan, 'catalog')
    workflows.snapshot('reads')
    workflows.load_plan('reads')
    workflows.agent_turns('reads')
    workflows.subtask_record('reads', 's0')
    workflows.policy_pin('reads')
    workflows.routing_hash('reads')
    workflows.replan_successor('reads')
    workflows.dispatched_count('reads')
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            connection.execute('SELECT 1')


@pytest.mark.parametrize('operation', ['write', 'acquire', 'release'])
def test_write_begin_failure_preserves_original_exception(tmp_path, monkeypatch, operation):
    store = SqliteStore(tmp_path/'locked.db')
    class BrokenConnection:
        in_transaction = False
        closed = False
        def execute(self, sql):
            if sql == 'BEGIN IMMEDIATE':
                raise sqlite3.OperationalError('database is locked')
            raise AssertionError('ROLLBACK must not mask BEGIN failure')
        def close(self):
            self.closed = True
    connection = BrokenConnection()
    monkeypatch.setattr(store, '_connect', lambda: connection)
    with pytest.raises(sqlite3.OperationalError, match='database is locked'):
        if operation == 'write':
            with store._write():
                pytest.fail('must not enter transaction')
        elif operation == 'acquire':
            store.acquire_worker('test-worker')
        else:
            from self_evolving_embodied.storage.sqlite.database import WorkerFence
            store.release_worker(WorkerFence('test-worker', 1, 10000))
    assert connection.closed


def test_policy_wait_state_is_persisted(tmp_path):
    class LocalBrain(Brain):
        def decide(self, context):
            return AgentDecision(AgentAction.CONTINUE, 'Continue')
    with _plain_framework(tmp_path/'policy.db', LocalBrain) as app:
        workflow = app.components.workflow.workflows
        with app.components.store.worker_session():
            workflow.create('budget', _plan(app.components.registry, 'Inspect'), 'catalog')
            workflow.wait('budget', 'WAITING_POLICY', 'RUNTIME_POLICY_TOOL_BUDGET_REACHED')
        assert workflow.snapshot('budget').status == 'WAITING_POLICY'


def test_failed_replan_link_rolls_back_successor_and_parent(tmp_path, monkeypatch):
    with builder(tmp_path, []).build() as app:
        app.publish_observation(ObservationInput(KEY, {'target_visible': False}))
        result = app.run_instruction('Inspect')
        workflow = app.components.workflow.workflows
        before = workflow.snapshot(result.root_run_id)
        def fail(*args):
            raise RuntimeError('simulated failure before link commit')
        monkeypatch.setattr(workflow, '_mark_replanned', fail)
        with app.components.store.worker_session(), pytest.raises(RuntimeError, match='simulated failure'):
            workflow.create_replan(result.root_run_id, 'uncommitted-child',
                _plan(app.components.registry, 'Inspect'), app.components.workflow.catalog_hash(),
                routing_manifest=app.components.agent._router.manifest())
        assert workflow.snapshot(result.root_run_id) == before
        with pytest.raises(DomainError, match='WORKFLOW_NOT_FOUND'):
            workflow.snapshot('uncommitted-child')


def test_replan_cannot_drop_pinned_routing(tmp_path):
    with builder(tmp_path, []).build() as app:
        app.publish_observation(ObservationInput(KEY, {'target_visible': False}))
        report = app.run_instruction('Inspect')
        workflow = app.components.workflow.workflows
        before = workflow.snapshot(report.root_run_id)
        with app.components.store.worker_session(), pytest.raises(DomainError, match='REPLAN_SUCCESSOR_CONFIGURATION_MISMATCH'):
            workflow.create_replan(report.root_run_id, 'different-routing',
                _plan(app.components.registry, 'Inspect'), app.components.workflow.catalog_hash())
        assert workflow.snapshot(report.root_run_id) == before
        with pytest.raises(DomainError, match='WORKFLOW_NOT_FOUND'):
            workflow.snapshot('different-routing')


def test_public_exports_and_package_version():
    import importlib
    import pkgutil
    import tomllib
    from pathlib import Path
    import self_evolving_embodied as package
    from importlib.metadata import version
    project = tomllib.loads((Path(__file__).parents[1]/'pyproject.toml').read_text(encoding='utf-8'))
    assert package.__version__ == project['project']['version'] == version('self-evolving-embodied')
    for entry in pkgutil.walk_packages(package.__path__, package.__name__+'.'):
        module = importlib.import_module(entry.name)
        names = getattr(module, '__all__', ())
        assert len(names) == len(set(names)), entry.name
        for name in names:
            assert hasattr(module, name), (entry.name, name)


def test_independent_sensor_capacity_and_retention(tmp_path, monkeypatch):
    from self_evolving_embodied.observations import pipeline as module
    now = [1_000_000_000]
    monkeypatch.setattr(module.time, 'monotonic_ns', lambda: now[0])
    p = QueueObservationPipeline()
    second = ObservationKey('imu', 'imu')
    p.register_stream(SensorStreamConfig(KEY, 30, capacity=2))
    p.register_stream(SensorStreamConfig(second, 100, retention_seconds=1))
    for i in range(3):
        p.push(ObservationInput(KEY, bytes([i])))
    p.push(ObservationInput(second, b'imu'))
    assert p.stream_stats(KEY).capacity == 2
    assert p.stream_stats(KEY).dropped_capacity == 1
    assert p.stream_stats(second).capacity == 100
    now[0] += 2_000_000_000
    assert p.latest(ObservationRequest(second)) is None
    assert len(p.window(ObservationRequest(KEY), 60)) == 2
    p.close()
    config = tmp_path/'streams.toml'
    config.write_text('[[streams]]\nsource_id="sensor"\nmodality="rgb"\nfrequency_hz=30\ncapacity=7\n', encoding='utf-8')
    loaded = pipeline_from_toml(config)
    assert loaded.stream_stats(ObservationKey('sensor', 'rgb')).capacity == 7
    loaded.close()


def test_source_clock_snapshot_and_scoped_reader():
    p = QueueObservationPipeline()
    other = ObservationKey('rear_camera', 'rgb')
    for key in (KEY, other):
        p.register_stream(SensorStreamConfig(key, 30))
    p.push(ObservationInput(KEY, b'front', source_timestamp_ns=1, time_domain='device'))
    p.push(ObservationInput(other, b'rear', source_timestamp_ns=2, time_domain='device'))
    snap = p.snapshot([ObservationRequest(KEY), ObservationRequest(other)])
    assert snap.synchronization is SynchronizationStatus.SOURCE_ALIGNED
    reader = ScopedObservationReader(p, [KEY])
    with pytest.raises(RuntimeError, match='not permitted'):
        reader.latest(ObservationRequest(other))
    p.close()


def test_concurrent_producers_and_shutdown_unblocks_subscriber():
    p = QueueObservationPipeline(PipelineConfig(max_buffer_bytes=None))
    p.register_stream(SensorStreamConfig(KEY, 100, capacity=500))
    records = []
    def producer():
        for _ in range(100):
            records.append(p.push(ObservationInput(KEY, b'frame')).sequence_id)
    writers = [Thread(target=producer) for _ in range(4)]
    for thread in writers:
        thread.start()
    for thread in writers:
        thread.join(5)
        assert not thread.is_alive()
    assert sorted(records) == list(range(1, 401))
    sub = p.subscribe(ObservationRequest(KEY))
    started, returned = Event(), Event()
    def wait_read():
        started.set()
        assert sub.read_next() is None
        returned.set()
    reader = Thread(target=wait_read, daemon=True)
    reader.start()
    assert started.wait(2)
    p.close()
    assert returned.wait(2)
    reader.join(2)


def test_framework_close_refuses_active_task_and_cleans_all_resources(tmp_path):
    entered, release = Event(), Event()
    outcomes, errors, events = [], [], []
    class LocalBrain(Brain):
        def decide(self, context):
            return AgentDecision(AgentAction.CONTINUE, 'Continue')
    class BlockingTool:
        def invoke(self, args):
            entered.set()
            assert release.wait(5)
            return {'tool_report_available': True}
    app = _plain_framework(tmp_path/'active.db', LocalBrain, BlockingTool()).start()
    def run():
        try:
            outcomes.append(app.run_instruction('Inspect'))
        except Exception as exc:
            errors.append(exc)
    worker = Thread(target=run)
    worker.start()
    try:
        assert entered.wait(2)
        with pytest.raises(DomainError, match='AGENT_OPERATION_ACTIVE'):
            app.close()
        with pytest.raises(DomainError, match='SINGLE_AGENT_OPERATION_ALREADY_ACTIVE'):
            app.run_instruction('Another')
    finally:
        release.set()
        worker.join(5)
        app.close()
    assert not errors and outcomes[0].status == 'EXECUTION_WORKFLOW_COMPLETED'
    assert app.state is FrameworkState.CLOSED
    app.close()
    class Resource:
        def __init__(self, name, fail=False):
            self.name, self.fail = name, fail
        def start(self):
            pass
        def close(self):
            events.append(self.name)
            if self.fail:
                raise RuntimeError('close failure')
    b = FrameworkBuilder(FrameworkConfig(tmp_path/'cleanup.db'), brain_factory=LocalBrain)
    b.add_managed_resource(lambda: Resource('first'))
    b.add_managed_resource(lambda: Resource('second', True))
    failed = b.build().start()
    with pytest.raises(DomainError, match='FRAMEWORK_RESOURCE_SHUTDOWN_FAILED'):
        failed.close()
    assert events == ['second', 'first'] and failed.state is FrameworkState.CLOSED


def test_ros_ingress_accepts_framework_sink_without_duplicate_pipeline(tmp_path):
    binding = ROS2TopicBinding('/synthetic', 'sensor_msgs/msg/Image', KEY, 30)
    ingress = []
    class LocalSource:
        def __init__(self, sink):
            self.port = ROS2TopicIngress(sink, (binding,))
            ingress.append(self.port)
        def start(self):
            self.port.accept('/synthetic', SimpleNamespace(data=b'pixels'))
        def close(self):
            self.port.close()
    b = FrameworkBuilder(FrameworkConfig(tmp_path/'ros.db'), brain_factory=Brain)
    b.add_stream(SensorStreamConfig(KEY, 30)).add_observation_source(LocalSource)
    with b.build() as app:
        assert app.observations.latest(ObservationRequest(KEY)).payload == b'pixels'
        assert app.components.observations.registered_streams() == (KEY,)
        assert ingress[0].stats().received == 1


def test_ros_close_attempts_every_subscription():
    p = QueueObservationPipeline()
    port = ROS2TopicIngress(p, (ROS2TopicBinding('/synthetic', 'sensor_msgs/msg/Image', KEY, 30),))
    destroyed = []
    class Node:
        def destroy_subscription(self, sub):
            destroyed.append(sub)
            if sub == 'first':
                raise RuntimeError('destroy failed')
    port._node, port._subscriptions = Node(), ['first', 'second']
    with pytest.raises(RuntimeError, match='destroy failed'):
        port.close()
    assert destroyed == ['first', 'second']
    port.close()
    p.close()


def test_ros_cancel_is_unknown_even_after_protocol_ack():
    from concurrent.futures import Future
    cancelled = []
    class Goal:
        accepted = True
        def get_result_async(self):
            reply = Future()
            reply.set_result({'protocol_status': 'completed'})
            return reply
        def cancel_goal_async(self):
            cancelled.append(True)
            reply = Future()
            reply.set_result({'cancel_ack': True})
            return reply
    class Client:
        def send_goal_async(self, goal):
            reply = Future()
            reply.set_result(Goal())
            return reply
    class Fence:
        def enforce_at_controller(self, ticket):
            return True
    ticket = ExecutionTicket('run', 'step', 'invoke', 'control.test', '1.0.0',
                             'control.test', 'done', 'stub', '0'*64, 1, 1.0)
    adapter = ROS2FencedActionAdapter(Client(), Fence(), lambda _t, args: args, lambda r: r, lambda _t: {})
    assert adapter.enforce_fencing(ticket)
    adapter.invoke(ticket, {})
    assert adapter.request_stop(ticket) == 'UNKNOWN'
    assert cancelled == [True]
    assert not adapter.enforce_fencing(ticket)


def test_assessment_late_result_is_discarded():
    release, completed = Event(), Event()
    box = AssessmentToolbox()
    spec = AssessorSpec('assessment.slow', 'assessment.test', 'Slow local assessor')
    class Slow:
        def assess(self, request, reader):
            assert release.wait(2)
            completed.set()
            return AssessmentResult(Outcome.SUPPORTED, spec.assessor_id, 'Late result')
    box.register(spec, Slow())
    try:
        with pytest.raises(DomainError, match='ASSESSMENT_PROVIDER_TIMEOUT'):
            box.assess(spec.assessor_id, AssessmentRequest('s', 'inspect', 'REPORTED_COMPLETED', {}), timeout_seconds=0.01)
    finally:
        release.set()
        assert completed.wait(2)


@pytest.mark.parametrize('value', [True, '30', None, float('inf'), float('nan'), 0])
def test_llm_timeout_config_rejects_invalid_values(value):
    with pytest.raises(DomainError, match='INVALID_LLM_CONFIG'):
        OpenAICompatibleConfig('https://local.example/v1', 'stub', timeout_seconds=value)


@pytest.mark.parametrize('fail', [False, True])
def test_optional_llm_client_is_closed_and_secrets_are_not_exposed(monkeypatch, fail):
    events = []
    class Client:
        def __init__(self, **kwargs):
            self.chat = SimpleNamespace(completions=self)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            events.append('closed')
        def create(self, **kwargs):
            if fail:
                raise RuntimeError('test-key-never-expose')
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='stub response'))])
    monkeypatch.setitem(sys.modules, 'openai', SimpleNamespace(OpenAI=Client))
    monkeypatch.setenv('LLM_API_KEY', 'test-key-never-expose')
    provider = OpenAICompatibleProvider(OpenAICompatibleConfig('https://local.example/v1', 'stub'))
    if fail:
        with pytest.raises(DomainError, match='LLM_REQUEST_FAILED') as exc:
            provider.complete('system', 'user')
        assert 'test-key' not in str(exc.value)
    else:
        assert provider.complete('system', 'user') == 'stub response'
    assert events == ['closed']


@pytest.mark.parametrize('timeout', [True, float('nan'), float('inf')])
def test_invalid_effectful_grant_timeout_is_rejected(timeout):
    with pytest.raises(DomainError, match='INVALID_EXECUTION_GRANT'):
        ExecutionGrant('control.stub', '1.0.0', 'stub', ('done',), timeout_seconds=timeout)


def test_raw_sensor_payload_is_not_in_workflow_database(tmp_path):
    class LocalBrain(Brain):
        def decide(self, context):
            return AgentDecision(AgentAction.CONTINUE, 'Continue')
    with builder(tmp_path, []).build() as app:
        app.publish_observation(ObservationInput(KEY, {'target_visible': True, 'private_pixels': 'RAW_SENTINEL_RC3'}))
        app.run_instruction('Inspect synthetic object')
        with closing(app.components.store._connect()) as db:
            dump = '\n'.join(db.iterdump())
            assert 'RAW_SENTINEL_RC3' not in dump
            assert db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'


@pytest.mark.parametrize('table,column', [
    ('agent_policy_studies', 'study_id'), ('agent_policy_reviews', 'decision_id'),
    ('agent_evolution_experiences', 'run_id'), ('agent_policy_release_events', 'event_id'),
])
def test_incomplete_offline_schema_rolls_back_migration(tmp_path, table, column):
    from test_rc2_reliability import _legacy_copy, _version
    path = tmp_path/'offline-shadow.db'
    _legacy_copy(path)
    with closing(sqlite3.connect(path)) as db:
        db.execute(f'CREATE TABLE {table} ({column} TEXT PRIMARY KEY)')
        db.commit()
    with pytest.raises(DomainError, match='INCOMPATIBLE_SCHEMA_LAYOUT'):
        SqliteStore(path)
    assert _version(path) == 12
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='agent_workflows'").fetchone() is None
