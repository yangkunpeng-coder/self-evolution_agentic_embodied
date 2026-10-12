"""Run one read-only framework task with a local Stub LLM and synthetic sensor."""
from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from self_evolving_embodied import (
    AssessmentRoute, Effect, FrameworkBuilder, FrameworkConfig, ObservationInput,
    ObservationKey, ObservationNeeds, ObservationRequest, SensorStreamConfig,
    ToolKind, ToolSpec,
)
from self_evolving_embodied.toolboxes.assessment import (
    AssessorSpec, SensorFieldRule, SensorFieldRuleAssessor,
)

KEY = ObservationKey('synthetic_sensor', 'metadata')
REQUEST = ObservationRequest(KEY, max_age_ms=5000)
NEEDS = ObservationNeeds(required=(REQUEST,))
TOOL = ToolSpec('perception.inspect', 'perception.inspect', ToolKind.CUSTOM,
                'Inspect synthetic metadata through ObservationReader',
                (('query', 'string'),), ('report_available',), effect=Effect.READ_ONLY)
ASSESSOR = AssessorSpec('assessment.visible', 'assessment.visibility',
    'Apply a rule to synthetic metadata', observation_needs=NEEDS,
    method='RULE', independent_from_execution=True)
ROUTE = AssessmentRoute(TOOL.tool_id, ASSESSOR.assessor_id)


class StubLLM:
    """Deterministic protocol fixture, replace with a host-owned LLM provider."""
    def complete(self, system_prompt: str, user_prompt: str) -> str:
        if 'high-level robotics task planner' in system_prompt:
            return json.dumps({'goal': user_prompt, 'subtasks': [{
                'id': 'inspect', 'tool_id': TOOL.tool_id,
                'arguments': {'query': user_prompt}, 'depends_on': [],
                'success_condition': 'report_available',
            }]})
        context = json.loads(user_prompt)
        outcome = (context.get('assessment') or {}).get('outcome')
        return json.dumps({'action': 'CONTINUE' if outcome == 'SUPPORTED' else 'WAIT_FOR_OBSERVATION',
                           'reason': 'Review synthetic observation evidence'})


class MetadataTool:
    def __init__(self, reader):
        self.reader = reader
    def invoke(self, arguments):
        record = self.reader.latest(REQUEST)
        # Return only compact metadata, never raw sensor payloads.
        return {'report_available': record is not None,
                'sequence_id': record.sequence_id if record else None}


def assess_factory():
    return SensorFieldRuleAssessor(ASSESSOR.assessor_id,
                                  SensorFieldRule(REQUEST, ('visible',), 'equals', True))


def report_policy(_arguments, output):
    # Receipt of a tool report is distinct from goal outcome.
    return output.get('report_available') is True


def main() -> None:
    with TemporaryDirectory(prefix='embodied-framework-') as folder:
        builder = FrameworkBuilder(FrameworkConfig(Path(folder)/'tasks.sqlite3'), provider=StubLLM())
        builder.add_stream(SensorStreamConfig(KEY, frequency_hz=30, capacity=1800))
        builder.add_execution_tool(TOOL, needs=NEEDS, factory=MetadataTool)
        builder.add_assessor(ASSESSOR, assess_factory()).add_assessment_route(ROUTE)
        builder.add_evidence_policy('report_available', report_policy)
        with builder.build() as framework:
            framework.publish_observation(ObservationInput(KEY, {'visible': True}))
            report = framework.run_instruction('Inspect synthetic object')
            assert report.status == 'EXECUTION_WORKFLOW_COMPLETED'
            print(json.dumps(asdict(report), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
