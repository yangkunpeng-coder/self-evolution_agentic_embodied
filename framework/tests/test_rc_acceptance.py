"""Framework-only RC acceptance: synthetic tools; no robotics or experiment claims."""
from __future__ import annotations
import json
from pathlib import Path

import pytest

from self_evolving_embodied import (
    FrameworkBuilder, FrameworkConfig, FrameworkState, DomainError,
    ObservationInput, ObservationKey, ObservationRequest, SensorStreamConfig,
    ToolSpec, ToolKind, Effect, ObservationNeeds, AssessmentRoute,
    TrustedPluginCatalog, PluginKind, PluginManifest,
    load_framework_settings, build_configured_framework,
)
from self_evolving_embodied.agent import AgentAction, AgentDecision
from self_evolving_embodied.planning.validator import PlanValidator
from self_evolving_embodied.observations import QueueObservationPipeline
from self_evolving_embodied.toolboxes.assessment import (
    AssessorSpec, AssessmentToolbox, AssessmentRequest,
    SensorFieldRule, SensorFieldRuleAssessor, Outcome,
)
from self_evolving_embodied.plugins.catalog import PluginSelection

KEY = ObservationKey('front_camera', 'rgb')
SPEC = ToolSpec('perception.rgb_summary', 'perception.describe', ToolKind.VLM,
                'Synthetic local metadata tool', (('query', 'string'),),
                ('tool_report_available',), effect=Effect.READ_ONLY)
NEEDS = ObservationNeeds(required=(ObservationRequest(KEY, max_age_ms=5000),))
ASSESS_SPEC = AssessorSpec('assessment.object_visible', 'assessment.visibility',
                           'Synthetic field assessor', observation_needs=NEEDS,
                           method='RULE', independent_from_execution=True)

class Brain:
    def __init__(self, registry):
        self.validator = PlanValidator(registry)
    def plan(self, goal):
        return self.validator.parse(json.dumps({'goal': goal, 'subtasks': [
            {'id': 'locate', 'tool_id': SPEC.tool_id, 'arguments': {'query': goal},
             'depends_on': [], 'success_condition': 'tool_report_available'}
        ]}), goal)
    def decide(self, context):
        if context.assessment and context.assessment['outcome'] == 'SUPPORTED':
            return AgentDecision(AgentAction.CONTINUE, 'Observation supports goal')
        return AgentDecision(AgentAction.WAIT_FOR_OBSERVATION, 'More observations needed')
    def replan(self, goal, context):
        return self.plan(goal)

class Tool:
    def __init__(self, reader):
        self.reader = reader
    def invoke(self, arguments):
        obs = self.reader.latest(ObservationRequest(KEY, max_age_ms=5000))
        return {'tool_report_available': obs is not None, 'sequence_id': obs.sequence_id if obs else None}

class Source:
    def __init__(self, sink, events):
        self.sink = sink
        self.events = events
    def start(self):
        self.events.append('source_start')
        self.sink.push(ObservationInput(KEY, {'target_visible': True}))
    def close(self):
        self.events.append('source_close')

class Managed:
    def __init__(self, events, fail=False):
        self.events = events
        self.fail = fail
    def start(self):
        self.events.append('managed_start')
        if self.fail:
            raise RuntimeError('intentional startup failure')
    def close(self):
        self.events.append('managed_close')


def builder(tmp_path, events, *, fail_resource=False):
    b = FrameworkBuilder(FrameworkConfig(database_path=tmp_path / 'runtime.sqlite3'),
                         brain_factory=Brain)
    b.add_stream(SensorStreamConfig(KEY, frequency_hz=30))
    b.add_execution_tool(SPEC, needs=NEEDS, factory=Tool)
    b.add_assessor(ASSESS_SPEC, SensorFieldRuleAssessor(ASSESS_SPEC.assessor_id,
        SensorFieldRule(ObservationRequest(KEY, 5000), ('target_visible',), 'equals', True)))
    b.add_assessment_route(AssessmentRoute(SPEC.tool_id, ASSESS_SPEC.assessor_id))
    b.add_evidence_policy('tool_report_available', lambda arguments, output: output.get('tool_report_available') is True)
    b.add_managed_resource(lambda: Managed(events, fail_resource))
    b.add_observation_source(lambda sink: Source(sink, events))
    return b


def test_single_agent_e2e_synthetic(tmp_path):
    events=[]
    framework = builder(tmp_path, events).build()
    assert framework.state is FrameworkState.CREATED
    inv=framework.inventory()
    assert len(inv.execution_tools)==1 and len(inv.assessment_tools)==1
    with framework:
        assert framework.state is FrameworkState.RUNNING
        assert framework.observations.latest(ObservationRequest(KEY)) is not None
        report=framework.run_instruction('Locate object')
        assert report.last_outcome == 'SUPPORTED'
        assert report.workflow_status == 'SUCCEEDED'
        assert report.completed_turns == 1
    assert framework.state is FrameworkState.CLOSED
    assert events == ['managed_start', 'source_start', 'source_close', 'managed_close']
    assert framework.components.observations.stream_stats(KEY).buffered_items == 0


def test_startup_failure_cleans_resources_and_closes(tmp_path):
    events=[]
    framework=builder(tmp_path,events,fail_resource=True).build()
    with pytest.raises(RuntimeError, match='intentional'):
        framework.start()
    assert framework.state is FrameworkState.CLOSED
    assert events == ['managed_start','managed_close']
    with pytest.raises(DomainError):
        framework.run_instruction('Cannot start')


def test_pipeline_queue_non_destructive_and_missing_assessment():
    p=QueueObservationPipeline()
    p.register_stream(SensorStreamConfig(KEY, frequency_hz=1, retention_seconds=1))
    first=p.push(ObservationInput(KEY, {'target_visible': True}))
    second=p.push(ObservationInput(KEY, {'target_visible': False}))
    assert second.sequence_id == first.sequence_id+1
    assert p.latest(ObservationRequest(KEY)).sequence_id == second.sequence_id
    assert p.latest(ObservationRequest(KEY)).sequence_id == second.sequence_id
    assert p.stream_stats(KEY).capacity >= 1
    toolbox=AssessmentToolbox(observations=p)
    toolbox.register(ASSESS_SPEC, SensorFieldRuleAssessor(ASSESS_SPEC.assessor_id,
        SensorFieldRule(ObservationRequest(KEY, 5000),('target_visible',),'equals',True)))
    req=AssessmentRequest('s1','visible?','REPORTED_COMPLETED',{})
    assert toolbox.assess(ASSESS_SPEC.assessor_id,req).outcome is Outcome.NOT_SUPPORTED
    p.close()


def test_assessor_unknown_without_required_observation():
    p=QueueObservationPipeline()
    p.register_stream(SensorStreamConfig(KEY,frequency_hz=10))
    box=AssessmentToolbox(observations=p)
    box.register(ASSESS_SPEC, SensorFieldRuleAssessor(ASSESS_SPEC.assessor_id,
        SensorFieldRule(ObservationRequest(KEY,5000),('target_visible',),'equals',True)))
    result=box.assess(ASSESS_SPEC.assessor_id,
        AssessmentRequest('s1','visible?','REPORTED_COMPLETED',{}))
    assert result.outcome is Outcome.UNKNOWN
    p.close()


def test_trusted_plugin_toml_configured_framework(tmp_path):
    settings_path = tmp_path / 'config.toml'
    settings_path.write_text('''
[framework]
database_path = 'task.sqlite3'
[[streams]]
source_id = 'front_camera'
modality = 'rgb'
frequency_hz = 30.0
[plugins]
brain = { id = 'host.brain', version = '1.0.0' }
execution = [{ id = 'host.tool', version = '1.0.0' }]
assessment = [{ id = 'host.assessor', version = '1.0.0' }]
sources = [{ id = 'host.camera', version = '1.0.0' }]
''', encoding='utf-8')
    settings=load_framework_settings(settings_path)
    events=[]
    catalog=TrustedPluginCatalog()
    catalog.register_brain_factory(PluginManifest('host.brain','1.0.0',PluginKind.BRAIN_FACTORY,'Brain'), Brain)
    catalog.register_execution(PluginManifest('host.tool','1.0.0',PluginKind.EXECUTION,'Tool'), SPEC,
                               needs=NEEDS,factory=Tool)
    catalog.register_assessor(PluginManifest('host.assessor','1.0.0',PluginKind.ASSESSMENT,'Assessor'), ASSESS_SPEC,
                              factory=lambda: SensorFieldRuleAssessor(ASSESS_SPEC.assessor_id,
                               SensorFieldRule(ObservationRequest(KEY,5000),('target_visible',),'equals',True)))
    catalog.register_observation_source(PluginManifest('host.camera','1.0.0',PluginKind.OBSERVATION_SOURCE,'Camera'),
                                        lambda sink: Source(sink,events))
    framework=build_configured_framework(settings,catalog)
    assert len(framework.inventory().execution_tools) == 1
    with framework:
        assert framework.observations.latest(ObservationRequest(KEY)) is not None
    assert events == ['source_start','source_close']
    with pytest.raises(DomainError):
        catalog.register_brain_factory(PluginManifest('host.other','1.0.0',PluginKind.BRAIN_FACTORY,'Other'), Brain)


def test_effectful_tool_handler_cannot_bypass_harness(tmp_path):
    effectful=ToolSpec('control.arm','control.act',ToolKind.CONTROLLER,'Actuator',( ('command','string'),),
                       ('done',),effect=Effect.PHYSICAL_ACTION)
    b=FrameworkBuilder(FrameworkConfig(tmp_path/'tasks.sqlite3'),brain_factory=Brain)
    b.add_execution_tool(effectful)
    fw=b.build()
    with fw:
        assert fw.inventory().effectful_bridge_configured is False
    bad=FrameworkBuilder(FrameworkConfig(tmp_path/'tasks2.sqlite3'),brain_factory=Brain)
    bad.add_execution_tool(effectful,factory=Tool)
    with pytest.raises(DomainError,match='EFFECTFUL_TOOL_REQUIRES_HARNESS'):
        bad.build()


def test_unknown_assessment_pauses_and_resumes_with_new_observation(tmp_path):
    events = []
    f = builder(tmp_path, events).build()
    with f:
        # Source published True, now publish False to make outcome NOT_SUPPORTED.
        f.publish_observation(ObservationInput(KEY, {'target_visible': False}))
        report = f.run_instruction('Find object')
        assert report.status == 'WAITING_OBSERVATION'
        assert report.last_outcome == 'NOT_SUPPORTED'
        f.publish_observation(ObservationInput(KEY, {'target_visible': True}))
        resumed = f.resume_instruction(report.root_run_id, retry_observation=True)
        assert resumed.status == 'EXECUTION_WORKFLOW_COMPLETED'
        assert resumed.last_outcome == 'SUPPORTED'
        assert resumed.completed_turns >= 2


def test_llm_provider_contract_without_network(tmp_path):
    class LocalReplyProvider:
        def __init__(self):
            self.calls=[]
        def complete(self, system_prompt, user_prompt):
            self.calls.append((system_prompt, user_prompt))
            if 'high-level robotics task planner' in system_prompt:
                return json.dumps({'goal': user_prompt, 'subtasks': [{
                    'id':'locate', 'tool_id': SPEC.tool_id, 'arguments':{'query':user_prompt},
                    'depends_on':[], 'success_condition':'tool_report_available'}]})
            return '{"action":"CONTINUE","reason":"Observation checked"}'
    provider=LocalReplyProvider()
    f=FrameworkBuilder(FrameworkConfig(tmp_path/'task.sqlite'),provider=provider)
    f.add_stream(SensorStreamConfig(KEY, frequency_hz=30))
    f.add_execution_tool(SPEC, needs=NEEDS, factory=Tool)
    f.add_evidence_policy('tool_report_available', lambda _a, out: out.get('tool_report_available') is True)
    app=f.build()
    with app:
        app.publish_observation(ObservationInput(KEY,{'target_visible':True}))
        report=app.run_instruction('Find cup')
        assert report.status == 'EXECUTION_WORKFLOW_COMPLETED'
    assert len(provider.calls)==2


def test_observation_default_sixty_seconds_and_memory_bound():
    from self_evolving_embodied.observations import PipelineConfig
    p=QueueObservationPipeline(PipelineConfig(max_buffer_bytes=5))
    p.register_stream(SensorStreamConfig(KEY, frequency_hz=30))
    assert p.stream_stats(KEY).capacity == 1800
    p.push(ObservationInput(KEY, b'one'))
    p.push(ObservationInput(KEY, b'two'))
    stats=p.stream_stats(KEY)
    assert stats.dropped_memory == 1
    assert p.latest(ObservationRequest(KEY)).payload == b'two'
    p.close()
    with pytest.raises(Exception):
        p.push(ObservationInput(KEY, b'bad'))


def test_observation_subscription_and_independent_reads():
    p=QueueObservationPipeline()
    p.register_stream(SensorStreamConfig(KEY, frequency_hz=2, retention_seconds=1))
    sub=p.subscribe(ObservationRequest(KEY))
    assert sub.read_next(timeout_seconds=0) is None
    p.push(ObservationInput(KEY, b'frame1'))
    observed=sub.read_next(timeout_seconds=0.1)
    assert observed.payload == b'frame1'
    assert p.latest(ObservationRequest(KEY)).payload == b'frame1'
    p.push(ObservationInput(KEY, b'frame2'))
    p.push(ObservationInput(KEY, b'frame3'))
    last=sub.read_next(timeout_seconds=0.1)
    assert last.sequence_id == 2 or last.sequence_id == 3
    assert p.stream_stats(KEY).buffered_items <= 2
    sub.close()
    p.close()


def test_undeclared_tool_sensor_fails_early(tmp_path):
    b=FrameworkBuilder(FrameworkConfig(tmp_path/'runtime.sqlite'),brain_factory=Brain)
    b.add_execution_tool(SPEC,needs=NEEDS,factory=Tool)
    with pytest.raises(DomainError,match='UNREGISTERED_TOOL_OBSERVATION_SOURCE'):
        b.build()
    assert not (tmp_path/'runtime.sqlite').exists()


def test_invalid_config_unknown_field_rejected(tmp_path):
    config=tmp_path/'bad.toml'
    config.write_text('''
[framework]
database_path='tasks.sqlite'
untrusted_python_module='system.run'
[plugins]
brain={id='host.brain',version='1.0.0'}
''',encoding='utf-8')
    with pytest.raises(DomainError,match='INVALID_FRAMEWORK_TOML'):
        load_framework_settings(config)
    assert not (tmp_path/'tasks.sqlite').exists()


def test_source_start_failure_unwinds_lifecycle(tmp_path):
    class FailingSource(Source):
        def start(self):
            self.events.append('source_start')
            raise RuntimeError('sensor startup failure')
    events=[]
    b=FrameworkBuilder(FrameworkConfig(tmp_path/'state.db'),brain_factory=Brain)
    b.add_managed_resource(lambda: Managed(events))
    b.add_observation_source(lambda sink: FailingSource(sink,events))
    f=b.build()
    with pytest.raises(RuntimeError,match='sensor startup failure'):
        f.start()
    assert events==['managed_start','source_start','source_close','managed_close']
    assert f.state is FrameworkState.CLOSED


def test_unbound_physical_action_is_not_a_callable_tool(tmp_path):
    from self_evolving_embodied.tools.registry import PluggableToolRegistry
    effectful=ToolSpec('control.arm','control.act',ToolKind.CONTROLLER,'Actuator',
                       (('command','string'),),('done',),effect=Effect.PHYSICAL_ACTION)
    registry=PluggableToolRegistry()
    registry.register(effectful)
    with pytest.raises(DomainError):
        registry.bind(effectful.tool_id, Tool(None))


def test_physical_handler_registration_is_rejected():
    from self_evolving_embodied.tools.registry import PluggableToolRegistry
    effectful=ToolSpec('control.arm','control.act',ToolKind.CONTROLLER,'Actuator',
                       (('command','string'),),('done',),effect=Effect.PHYSICAL_ACTION)
    r=PluggableToolRegistry()
    with pytest.raises(DomainError,match='EFFECTFUL_TOOL_REQUIRES_HARNESS'):
        r.register(effectful,Tool(None))
    assert not r.catalog()
