"""Optional TOML configuration for in-memory stream retention and capacities."""
from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from .contracts import ObservationKey, PipelineConfig, SensorStreamConfig
from .pipeline import QueueObservationPipeline


def pipeline_from_toml(path: str | Path) -> QueueObservationPipeline:
    with Path(path).open("rb") as file:
        data: dict[str, Any] = tomllib.load(file)
    if set(data) - {"observation_pipeline", "streams"}:
        raise ValueError("unknown observation configuration field")
    common = data.get("observation_pipeline", {})
    if not isinstance(common, dict):
        raise ValueError("[observation_pipeline] must be a TOML table")
    config = PipelineConfig(**common)
    pipeline = QueueObservationPipeline(config)
    streams = data.get("streams", [])
    if not isinstance(streams, list):
        raise ValueError("[[streams]] must be a TOML array of tables")
    try:
        for entry in streams:
            if not isinstance(entry, dict):
                raise ValueError("each [[streams]] must be a TOML table")
            entry = dict(entry)
            key = ObservationKey(source_id=entry.pop("source_id"),
                                 modality=entry.pop("modality"))
            pipeline.register_stream(SensorStreamConfig(key=key, **entry))
    except BaseException:
        pipeline.close()
        raise
    return pipeline
