"""Strict declarative TOML config; never resolves arbitrary Python import paths.

No passwords, tokens, grants or arbitrary plugin kwargs are accepted. External
credentials remain in trusted host code / provider-specific secret managers.
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import Any

from ..application.core import FrameworkConfig
from ..core.contracts import DomainError
from ..observations import ObservationKey, PipelineConfig, SensorStreamConfig
from .catalog import PluginSelection


@dataclass(frozen=True)
class FrameworkSettings:
    framework: FrameworkConfig
    streams: tuple[SensorStreamConfig, ...]
    brain: PluginSelection
    execution: tuple[PluginSelection, ...] = ()
    assessment: tuple[PluginSelection, ...] = ()
    sources: tuple[PluginSelection, ...] = ()


def _table(value: object, allowed: set[str], name: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) - allowed:
        raise DomainError("INVALID_FRAMEWORK_TOML", name)
    return value


def _selection(value: object) -> PluginSelection:
    row = _table(value, {"id", "version"}, "plugin selection")
    if set(row) != {"id", "version"}:
        raise DomainError("INVALID_PLUGIN_SELECTION")
    return PluginSelection(plugin_id=row["id"], version=row["version"])


def _selections(value: object, name: str) -> tuple[PluginSelection, ...]:
    if type(value) is not list or len(value) > 128:
        raise DomainError("INVALID_FRAMEWORK_TOML", name)
    selected = tuple(_selection(x) for x in value)
    if len({x.plugin_id for x in selected}) != len(selected):
        raise DomainError("DUPLICATE_PLUGIN_SELECTION")
    return selected


def load_framework_settings(path: str | Path) -> FrameworkSettings:
    """Load settings from a trusted local TOML document, without side effects.

    A relative database_path is resolved against the TOML file's directory.
    Windows drive and UNC paths retain their original Windows interpretation.
    """
    filename = Path(path)
    try:
        with filename.open("rb") as fh:
            document = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise DomainError("FRAMEWORK_TOML_READ_FAILED", str(exc)) from exc
    root = _table(document, {"framework", "observation_pipeline", "streams", "plugins"}, "root")
    framework_row = _table(root.get("framework", {}),
                           {"database_path", "max_turns", "max_replans"}, "framework")
    if "database_path" not in framework_row:
        raise DomainError("FRAMEWORK_DATABASE_PATH_REQUIRED")
    db_path = framework_row["database_path"]
    if not isinstance(db_path, str) or not db_path.strip():
        raise DomainError("INVALID_FRAMEWORK_DATABASE_PATH")
    if not Path(db_path).is_absolute() and not PureWindowsPath(db_path).is_absolute():
        db_path = str(filename.resolve().parent / db_path)
    pipeline_row = _table(root.get("observation_pipeline", {}),
                          {"default_retention_seconds", "max_buffer_bytes", "max_stream_capacity"},
                          "observation_pipeline")
    try:
        pipeline = PipelineConfig(**pipeline_row)
        config = FrameworkConfig(database_path=db_path, pipeline=pipeline,
                                 max_turns=framework_row.get("max_turns", 40),
                                 max_replans=framework_row.get("max_replans", 2))
    except (ValueError, TypeError) as exc:
        raise DomainError("INVALID_FRAMEWORK_TOML", "framework/pipeline") from exc
    rows = root.get("streams", [])
    if type(rows) is not list or len(rows) > 256:
        raise DomainError("INVALID_FRAMEWORK_TOML", "streams")
    streams = []
    for row in rows:
        sensor = _table(row, {"source_id", "modality", "frequency_hz", "retention_seconds", "capacity"}, "stream")
        if not {"source_id", "modality", "frequency_hz"} <= set(sensor):
            raise DomainError("INVALID_FRAMEWORK_TOML", "stream")
        try:
            streams.append(SensorStreamConfig(
                key=ObservationKey(sensor["source_id"], sensor["modality"]),
                frequency_hz=sensor["frequency_hz"],
                retention_seconds=sensor.get("retention_seconds"),
                capacity=sensor.get("capacity"),
            ))
        except (ValueError, TypeError) as exc:
            raise DomainError("INVALID_FRAMEWORK_TOML", "stream") from exc
    if len({stream.key for stream in streams}) != len(streams):
        raise DomainError("DUPLICATE_SENSOR_STREAM")
    plugins = _table(root.get("plugins", {}),
                     {"brain", "execution", "assessment", "sources"}, "plugins")
    if "brain" not in plugins:
        raise DomainError("FRAMEWORK_BRAIN_PLUGIN_REQUIRED")
    brain = _selection(plugins["brain"])
    execution = _selections(plugins.get("execution", []), "execution")
    assessment = _selections(plugins.get("assessment", []), "assessment")
    sources = _selections(plugins.get("sources", []), "sources")
    all_ids = [brain.plugin_id] + [s.plugin_id for group in (execution, assessment, sources) for s in group]
    if len(all_ids) != len(set(all_ids)):
        raise DomainError("DUPLICATE_PLUGIN_SELECTION")
    return FrameworkSettings(config, tuple(streams), brain, execution, assessment, sources)
