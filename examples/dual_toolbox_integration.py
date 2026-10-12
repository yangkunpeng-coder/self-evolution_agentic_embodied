"""Lightweight usage example for the dual toolbox APIs.

No ROS 2, LLM, VLA or simulator is needed. Synthetic observations merely
illustrate interface wiring, not real robot task success or a verified trial.

From the repository root: python examples/dual_toolbox_integration.py
"""
from __future__ import annotations

from typing import Any, Mapping

from self_evolving_embodied.observations import (
    ObservationInput, ObservationKey, ObservationReader,
    ObservationRequest, QueueObservationPipeline, SensorStreamConfig,
)
from self_evolving_embodied.tools import Effect, PluggableToolRegistry, ToolKind, ToolSpec
from self_evolving_embodied.toolboxes import ObservationNeeds
from self_evolving_embodied.toolboxes.execution import ExecutionToolbox
from self_evolving_embodied.toolboxes.assessment import (
    AssessmentRequest, AssessmentToolbox, AssessorSpec,
    SensorFieldRule, SensorFieldRuleAssessor, assessment_recorded_policy,
)


class SensorMetadataProvider:
    """Stand-in for a real read-only VLM or conventional perception tool."""

    def __init__(self, reader: ObservationReader | None, key: ObservationKey) -> None:
        self._reader = reader
        self._key = key

    def invoke(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        assert self._reader is not None
        observation = self._reader.latest(ObservationRequest(self._key, max_age_ms=1000))
        return {"query": arguments["query"],
                "observation_available": observation is not None,
                "sequence_id": observation.sequence_id if observation is not None else None}


def main() -> None:
    rgb = ObservationKey("front_camera", "rgb")
    joint = ObservationKey("robot_arm", "joint_state")
    pipeline = QueueObservationPipeline()
    pipeline.register_stream(SensorStreamConfig(rgb, frequency_hz=30))
    pipeline.register_stream(SensorStreamConfig(joint, frequency_hz=100))

    # Real ROS 2 subscribers would push messages on arrival instead.
    pipeline.push(ObservationInput(rgb, payload=b"synthetic-frame"))
    pipeline.push(ObservationInput(joint, payload={"gripper_holding": True}))

    # Existing registry and orchestrator are REUSED, not duplicated.
    registry = PluggableToolRegistry()
    execution = ExecutionToolbox(registry, observations=pipeline)
    execution.register(
        ToolSpec(
            tool_id="perception.image_metadata", capability="perception.describe",
            kind=ToolKind.VLM, description="Read RGB frame metadata",
            argument_types=(("query", "string"),),
            success_conditions=("metadata_available",), effect=Effect.READ_ONLY,
        ),
        needs=ObservationNeeds(required=(ObservationRequest(rgb, max_age_ms=1000),)),
        factory=lambda reader: SensorMetadataProvider(reader, rgb),
    )
    report = registry.dispatch_read_only("perception.image_metadata", {"query": "cup"})

    assessments = AssessmentToolbox(observations=pipeline)
    assessor_id = "grasp.sensor_rule"
    joint_request = ObservationRequest(joint, max_age_ms=1000)
    assessments.register(
        AssessorSpec(
            assessor_id, "assessment.grasp", "Compare the observed gripper state to a rule",
            observation_needs=ObservationNeeds(required=(joint_request,)),
        ),
        SensorFieldRuleAssessor(
            assessor_id, SensorFieldRule(joint_request, ("gripper_holding",), "equals", True),
        ),
    )
    assessments.mount_as_harness_tool(assessor_id, registry, tool_id="assessment.grasp_rule")
    request = AssessmentRequest(
        subtask_id="grasp_cup", goal="hold the cup", execution_status="REPORTED_COMPLETED",
        tool_report={"source": "synthetic"},
    )
    # Standalone dispatch shown for API illustration. For durable audit, call
    # the same registered tool through WorkflowOrchestrator + trusted policy.
    result = registry.dispatch_read_only("assessment.grasp_rule", {"request": request.to_dict()})
    print("execution:", report.output)
    print("assessment:", result.output)
    print("assessment-recorded policy:", assessment_recorded_policy({}, result.output))
    print("WARNING: synthetic record is not evidence of real physical success")
    pipeline.close()


if __name__ == "__main__":
    main()
