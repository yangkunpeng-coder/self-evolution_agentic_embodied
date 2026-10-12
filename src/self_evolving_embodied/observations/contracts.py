"""Contracts for a platform-independent, in-memory observation stream.

A source timestamp is not assumed to share a clock with any other sensor.
Payloads should be immutable (for example bytes or a read-only array) while
published, because records are shared across readers without copying.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Protocol, Sequence

_SOURCE_RE = re.compile(r"^[A-Za-z0-9_./-]{1,128}$")


def _positive_finite(value: float | int, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{field_name} must be a positive finite number")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{field_name} must be a positive finite number")
    return number


@dataclass(frozen=True, order=True)
class ObservationKey:
    source_id: str
    modality: str

    def __post_init__(self) -> None:
        if (not isinstance(self.source_id, str) or not _SOURCE_RE.fullmatch(self.source_id)
                or not isinstance(self.modality, str) or not _SOURCE_RE.fullmatch(self.modality)):
            raise ValueError("source_id and modality must be nonempty valid identifiers")


@dataclass(frozen=True)
class PipelineConfig:
    default_retention_seconds: float = 60.0
    max_buffer_bytes: int | None = 512 * 1024 * 1024
    max_stream_capacity: int = 1_000_000

    def __post_init__(self) -> None:
        _positive_finite(self.default_retention_seconds, "default_retention_seconds")
        if (self.max_buffer_bytes is not None
                and (type(self.max_buffer_bytes) is not int or self.max_buffer_bytes <= 0)):
            raise ValueError("max_buffer_bytes must be positive or None")
        if type(self.max_stream_capacity) is not int or self.max_stream_capacity < 1:
            raise ValueError("max_stream_capacity must be a positive integer")


@dataclass(frozen=True)
class SensorStreamConfig:
    key: ObservationKey
    frequency_hz: float
    retention_seconds: float | None = None
    capacity: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, ObservationKey):
            raise TypeError("key must be an ObservationKey")
        _positive_finite(self.frequency_hz, "frequency_hz")
        if self.retention_seconds is not None:
            _positive_finite(self.retention_seconds, "retention_seconds")
        if self.capacity is not None and (type(self.capacity) is not int or self.capacity < 1):
            raise ValueError("capacity must be a positive integer or None")


@dataclass(frozen=True)
class ObservationInput:
    key: ObservationKey
    payload: Any
    source_timestamp_ns: int | None = None
    time_domain: str | None = None
    frame_id: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)
    payload_size_bytes: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, ObservationKey):
            raise TypeError("key must be an ObservationKey")
        if self.source_timestamp_ns is not None and (type(self.source_timestamp_ns) is not int
                                                     or self.source_timestamp_ns < 0):
            raise ValueError("source_timestamp_ns must be a nonnegative integer or None")
        if self.time_domain is not None and (not isinstance(self.time_domain, str)
                                             or not self.time_domain.strip()):
            raise ValueError("time_domain must be a nonempty string or None")
        if self.frame_id is not None and not isinstance(self.frame_id, str):
            raise TypeError("frame_id must be a string or None")
        if self.payload_size_bytes is not None and (type(self.payload_size_bytes) is not int
                                                     or self.payload_size_bytes < 0):
            raise ValueError("payload_size_bytes must be a nonnegative integer or None")
        if not isinstance(self.metadata, Mapping) or any(
            not isinstance(k, str) or not isinstance(v, str)
            for k, v in self.metadata.items()
        ):
            raise ValueError("metadata must map strings to strings")


@dataclass(frozen=True)
class Observation:
    key: ObservationKey
    sequence_id: int
    received_monotonic_ns: int
    source_timestamp_ns: int | None
    time_domain: str | None
    frame_id: str | None
    payload: Any
    metadata: Mapping[str, str]
    approximate_size_bytes: int


@dataclass(frozen=True)
class ObservationRequest:
    key: ObservationKey
    max_age_ms: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, ObservationKey):
            raise TypeError("key must be an ObservationKey")
        if self.max_age_ms is not None:
            _positive_finite(self.max_age_ms, "max_age_ms")


class SynchronizationStatus(StrEnum):
    SOURCE_ALIGNED = "SOURCE_ALIGNED"  # matching declared source time domain
    ARRIVAL_ALIGNED_APPROXIMATE = "ARRIVAL_ALIGNED_APPROXIMATE"
    OUT_OF_TOLERANCE = "OUT_OF_TOLERANCE"
    INCOMPLETE = "INCOMPLETE"
    SINGLE_SOURCE = "SINGLE_SOURCE"


@dataclass(frozen=True)
class ObservationSnapshot:
    observations: tuple[Observation, ...]
    missing: tuple[ObservationKey, ...]
    stale: tuple[ObservationKey, ...]
    synchronization: SynchronizationStatus
    time_basis: str | None
    skew_ms: float | None

    def get(self, key: ObservationKey) -> Observation | None:
        return next((record for record in self.observations if record.key == key), None)


@dataclass(frozen=True)
class StreamStatistics:
    key: ObservationKey
    capacity: int
    retention_seconds: float
    frequency_hz: float
    buffered_items: int
    approximate_buffer_bytes: int
    received_total: int
    dropped_capacity: int
    dropped_retention: int
    dropped_memory: int


class ObservationSubscription(Protocol):
    @property
    def missed_messages(self) -> int: ...

    def read_next(self, timeout_seconds: float | None = None) -> Observation | None: ...

    def close(self) -> None: ...


class ObservationReader(Protocol):
    """The sole observation dependency required by execution/assessment tools."""

    def latest(self, request: ObservationRequest) -> Observation | None: ...

    def window(self, request: ObservationRequest, duration_seconds: float) -> tuple[Observation, ...]: ...

    def snapshot(self, requests: Sequence[ObservationRequest],
                 sync_tolerance_ms: float = 50.0) -> ObservationSnapshot: ...

    def subscribe(self, request: ObservationRequest) -> ObservationSubscription: ...


class ObservationSink(Protocol):
    """Ingress boundary for ROS 2, simulator, and hardware SDK adapters."""

    def push(self, data: ObservationInput) -> Observation: ...
