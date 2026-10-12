"""Interface-only demo of dev5 ONE-agent online execution loop.

Uses synthetic observation and deterministic, local Brain purely as an example.
No physical robot, ROS 2, online LLM, or true semantic success is demonstrated.
This file is NOT a validation test. See README.md for the framework boundaries.

From the repository root: python examples/single_agent_execution_loop.py
"""
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping

from self_evolving_embodied.agent import (
    AgentAction, AgentDecision, AssessmentRoute, BrainContext, SingleAgentExecutionLoop,
)
from self_evolving_embodied.observations import (
    ObservationInput, ObservationKey, ObservationReader, ObservationRequest,
    QueueObservationPipeline, SensorStreamConfig,
)
from self_evolving_embodied.planning.validator import PlanValidator
from self_evolving_embodied.runtime.workflow_orchestrator import WorkflowOrchestrator
from self_evolving_embodied.storage.sqlite.database import SqliteStore
from self_evolving_embodied.toolboxes import ObservationNeeds
from self_evolving_embodied.toolboxes.assessment import (
    AssessmentToolbox, AssessorSpec, SensorFieldRule, SensorFieldRuleAssessor,
)
from self_evolving_embodied.toolboxes.execution import ExecutionToolbox
from self_evolving_embodied.tools import Effect, PluggableToolRegistry, ToolKind, ToolSpec


class DemoVisionTool:
    def __init__(self, reader: ObservationReader | None, key: ObservationKey):
        self.reader = reader
        self.key = key

    def invoke(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.reader is None:
            raise RuntimeError("observation reader missing")
        frame = self.reader.latest(ObservationRequest(self.key, 5000))
        return {"tool_report_available": frame is not None,
                "query": arguments["query"],
                "frame_sequence": frame.sequence_id if frame is not None else None}


class DemoBrain:
    """Local protocol example; substitute LLMSingleAgentBrain for the real brain."""
    def __init__(self, registry: PluggableToolRegistry):
        self.validator = PlanValidator(registry)

    def plan(self, goal: str):
        import json
        return self.validator.parse(json.dumps({"goal": goal, "subtasks": [
            {"id": "locate", "tool_id": "perception.rgb_summary",
             "arguments": {"query": goal}, "depends_on": [],
             "success_condition": "tool_report_available"}
        ]}), goal)

    def decide(self, context: BrainContext) -> AgentDecision:
        if context.assessment is None or context.assessment["outcome"] != "SUPPORTED":
            return AgentDecision(AgentAction.WAIT_FOR_OBSERVATION,
                                 "Needs stronger observation")
        return AgentDecision(AgentAction.CONTINUE, "Assessment supports observation goal")

    def replan(self, goal: str, context: BrainContext):
        return self.plan(goal)


def main() -> None:
    rgb = ObservationKey("front_camera", "rgb")
    pipeline = QueueObservationPipeline()
    pipeline.register_stream(SensorStreamConfig(rgb, frequency_hz=30))
    # Synthetic payload has a label that would NOT exist in unprocessed camera pixels.
    pipeline.push(ObservationInput(rgb, payload={"target_visible": True}))

    registry = PluggableToolRegistry()
    execution = ExecutionToolbox(registry, observations=pipeline)
    execution.register(
        ToolSpec("perception.rgb_summary", "perception.describe", ToolKind.VLM,
                 "Synthetic observation metadata provider (NOT a VLM)",
                 (("query", "string"),), ("tool_report_available",),
                 effect=Effect.READ_ONLY),
        needs=ObservationNeeds(required=(ObservationRequest(rgb, max_age_ms=5000),)),
        factory=lambda reader: DemoVisionTool(reader, rgb),
    )
    assessments = AssessmentToolbox(observations=pipeline)
    rule = SensorFieldRule(ObservationRequest(rgb, max_age_ms=5000),
                           ("target_visible",), "equals", True)
    assessments.register(
        AssessorSpec("assessment.object_visible", "assessment.visibility",
                     "Apply an explicit rule to synthetic observations",
                     observation_needs=ObservationNeeds(required=(rule.observation,))),
        SensorFieldRuleAssessor("assessment.object_visible", rule),
    )
    # The Harness evidence policy verifies only receipt of a well-formed tool
    # report, NOT semantic visibility or physical task completion.
    evidence_policy = lambda _args, output: output.get("tool_report_available") is True
    with TemporaryDirectory() as folder:
        store = SqliteStore(Path(folder) / "workflow.sqlite")
        orchestrator = WorkflowOrchestrator(
            store, registry, {"tool_report_available": evidence_policy},
            require_agent_feedback=True,
        )
        agent = SingleAgentExecutionLoop(
            DemoBrain(registry), orchestrator, assessments,
            routes=[AssessmentRoute("perception.rgb_summary", "assessment.object_visible")],
        )
        report = agent.start("观察前方的目标物体")
        print(report)
        print("Synthetic outcome assessment is NOT a real robot success label")
    pipeline.close()


if __name__ == "__main__":
    main()
