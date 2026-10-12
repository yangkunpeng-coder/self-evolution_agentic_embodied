"""API usage example; uses synthetic messages, not a device or model.

Run from an editable framework install, e.g.:
    python examples/observation_tool_integration.py
"""
from __future__ import annotations

from typing import Any, Mapping

from self_evolving_embodied.observations import (
    ObservationInput, ObservationKey, ObservationReader, ObservationRequest,
    QueueObservationPipeline, ScopedObservationReader, SensorStreamConfig,
)
from self_evolving_embodied.tools.contracts import Effect, ToolKind, ToolSpec
from self_evolving_embodied.tools.registry import PluggableToolRegistry


class ImageMetadataTool:
    """A registered read-only tool that requests data through a reader Protocol."""

    def __init__(self, reader: ObservationReader) -> None:
        self._reader = reader

    def invoke(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        observation = self._reader.latest(
            ObservationRequest(ObservationKey("front_camera", "rgb"), max_age_ms=1000),
        )
        return {
            "query": str(arguments["query"]),
            "available": observation is not None,
            "sequence_id": observation.sequence_id if observation is not None else None,
        }


def main() -> None:
    pipeline = QueueObservationPipeline()
    key = ObservationKey("front_camera", "rgb")
    pipeline.register_stream(SensorStreamConfig(key, frequency_hz=30.0))

    reader = ScopedObservationReader(pipeline, allowed=[key])
    registry = PluggableToolRegistry()
    registry.register(
        ToolSpec(
            tool_id="perception.image_metadata",
            capability="perception.image_metadata",
            kind=ToolKind.CUSTOM,
            description="Report whether a fresh RGB observation is present.",
            argument_types=(("query", "string"),),
            success_conditions=("observation_read",),
            effect=Effect.READ_ONLY,
        ),
        ImageMetadataTool(reader),
    )

    subscriber = reader.subscribe(ObservationRequest(key))
    # A real ROS 2 / simulator adapter would push each new sensor message here.
    pipeline.push(ObservationInput(key=key, payload=b"synthetic rgb sample",
                                   source_timestamp_ns=1, time_domain="example"))
    print("subscription:", subscriber.read_next(timeout_seconds=0.0))
    print("tool:", registry.dispatch_read_only("perception.image_metadata", {"query": "latest"}))
    print("stream:", pipeline.stream_stats(key))
    subscriber.close()
    pipeline.close()


if __name__ == "__main__":
    main()
