"""Thread-safe, push-based observation pipeline backed by bounded deques.

No database, ROS dependency, background polling thread, or model inference.
Tool readers never consume/remove shared observations. This is a streaming
application-layer buffer, *not* a hard-real-time robot controller.
"""
from __future__ import annotations

import math
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from threading import Condition, RLock
from types import MappingProxyType
from typing import Any, Iterable, Sequence

from .contracts import (
    Observation, ObservationInput, ObservationKey, ObservationReader,
    ObservationRequest, ObservationSnapshot, ObservationSubscription,
    PipelineConfig, SensorStreamConfig, StreamStatistics, SynchronizationStatus,
    _positive_finite,
)


class ObservationPipelineError(RuntimeError):
    """Invalid operation on the runtime observation buffer."""


@dataclass
class _StreamState:
    config: SensorStreamConfig
    capacity: int
    retention_ns: int
    queue: deque[Observation] = field(default_factory=deque)
    last_sequence: int = 0
    approximate_bytes: int = 0
    dropped_capacity: int = 0
    dropped_retention: int = 0
    dropped_memory: int = 0


class QueueObservationPipeline:
    """Per-source, per-modality queues with wall-independent retention.

    Configure/register streams before receiving data. Sources call push() on
    arrival. Each record receives an independent sequence ID and monotonic
    reception time. Global memory accounting is approximate; a memory limit
    may shorten the 60-second retention window for large modalities.
    """

    def __init__(self, config: PipelineConfig | None = None) -> None:
        self.config = config or PipelineConfig()
        self._condition = Condition(RLock())
        self._streams: dict[ObservationKey, _StreamState] = {}
        self._approximate_bytes = 0
        self._closed = False

    def register_stream(self, config: SensorStreamConfig) -> None:
        capacity, retention_ns = self._derive_limits(config)
        with self._condition:
            self._ensure_open()
            if config.key in self._streams:
                raise ObservationPipelineError(f"stream already registered: {config.key}")
            self._streams[config.key] = _StreamState(config, capacity, retention_ns)

    def reconfigure_stream(self, config: SensorStreamConfig) -> None:
        """Change frequency/retention without resetting sequence counters."""
        capacity, retention_ns = self._derive_limits(config)
        with self._condition:
            self._ensure_open()
            state = self._state(config.key)
            state.config = config
            state.capacity = capacity
            state.retention_ns = retention_ns
            self._evict_expired(state, time.monotonic_ns())
            while len(state.queue) > capacity:
                self._drop_left(state, "capacity")
            self._condition.notify_all()

    def push(self, data: ObservationInput) -> Observation:
        """Called directly by a source adapter as soon as a message arrives.

        An oversized single payload is rejected rather than silently leaving
        an unbounded queue. Other memory pressure evicts the globally oldest
        retained records. All subscriber reads remain non-destructive.
        """
        if not isinstance(data, ObservationInput):
            raise TypeError("push requires ObservationInput")
        size = _estimate_size(data.payload, data.payload_size_bytes)
        with self._condition:
            self._ensure_open()
            state = self._state(data.key)
            receipt_ns = time.monotonic_ns()
            budget = self.config.max_buffer_bytes
            if budget is not None and size > budget:
                raise ObservationPipelineError("single observation exceeds max_buffer_bytes")
            self._evict_expired(state, receipt_ns)
            if len(state.queue) >= state.capacity:
                self._drop_left(state, "capacity")
            state.last_sequence += 1
            record = Observation(
                key=data.key,
                sequence_id=state.last_sequence,
                received_monotonic_ns=receipt_ns,
                source_timestamp_ns=data.source_timestamp_ns,
                time_domain=data.time_domain,
                frame_id=data.frame_id,
                payload=data.payload,
                metadata=MappingProxyType(dict(data.metadata)),
                approximate_size_bytes=size,
            )
            state.queue.append(record)
            state.approximate_bytes += size
            self._approximate_bytes += size
            if budget is not None:
                self._enforce_memory_budget(budget)
            self._condition.notify_all()
            return record

    def latest(self, request: ObservationRequest) -> Observation | None:
        self._validate_request(request)
        with self._condition:
            state = self._state(request.key)
            now_ns = time.monotonic_ns()
            self._evict_expired(state, now_ns)
            if not state.queue:
                return None
            record = state.queue[-1]
            return record if not _is_stale(record, request, now_ns) else None

    def window(self, request: ObservationRequest, duration_seconds: float) -> tuple[Observation, ...]:
        self._validate_request(request)
        duration = _positive_finite(duration_seconds, "duration_seconds")
        with self._condition:
            state = self._state(request.key)
            now_ns = time.monotonic_ns()
            self._evict_expired(state, now_ns)
            since = now_ns - int(duration * 1_000_000_000)
            return tuple(
                obs for obs in state.queue
                if obs.received_monotonic_ns >= since and not _is_stale(obs, request, now_ns)
            )

    def snapshot(self, requests: Sequence[ObservationRequest],
                 sync_tolerance_ms: float = 50.0) -> ObservationSnapshot:
        """Select latest records atomically and *report* alignment quality.

        Source-time synchronization is claimed only when all source timestamps
        have the same explicit domain. Otherwise arrival times are a clearly
        labeled approximation. This never invents a successful sensor sync.
        """
        tolerance_ms = _positive_finite(sync_tolerance_ms, "sync_tolerance_ms")
        items = tuple(requests)
        if not items:
            raise ValueError("snapshot requests cannot be empty")
        if len(set(item.key for item in items)) != len(items):
            raise ValueError("snapshot contains duplicate observation keys")
        for req in items:
            self._validate_request(req)
        with self._condition:
            now_ns = time.monotonic_ns()
            records: list[Observation] = []
            missing: list[ObservationKey] = []
            stale: list[ObservationKey] = []
            for req in items:
                state = self._state(req.key)
                self._evict_expired(state, now_ns)
                if not state.queue:
                    missing.append(req.key)
                    continue
                record = state.queue[-1]
                records.append(record)
                if _is_stale(record, req, now_ns):
                    stale.append(req.key)
            if missing:
                return ObservationSnapshot(tuple(records), tuple(missing), tuple(stale),
                                           SynchronizationStatus.INCOMPLETE, None, None)
            if len(records) == 1:
                return ObservationSnapshot(tuple(records), (), tuple(stale),
                                           SynchronizationStatus.SINGLE_SOURCE, None, 0.0)
            domains = {record.time_domain for record in records}
            source_compatible = (len(domains) == 1 and None not in domains
                                 and all(record.source_timestamp_ns is not None
                                         for record in records))
            if source_compatible:
                timestamps = [record.source_timestamp_ns for record in records]
                basis = f"source:{records[0].time_domain}"
            else:
                timestamps = [record.received_monotonic_ns for record in records]
                basis = "arrival_monotonic_approximate"
            skew_ms = (max(timestamps) - min(timestamps)) / 1_000_000
            if skew_ms > tolerance_ms:
                status = SynchronizationStatus.OUT_OF_TOLERANCE
            elif source_compatible:
                status = SynchronizationStatus.SOURCE_ALIGNED
            else:
                status = SynchronizationStatus.ARRIVAL_ALIGNED_APPROXIMATE
            return ObservationSnapshot(tuple(records), (), tuple(stale), status, basis, skew_ms)

    def subscribe(self, request: ObservationRequest) -> ObservationSubscription:
        self._validate_request(request)
        with self._condition:
            state = self._state(request.key)
            self._ensure_open()
            return _QueueSubscription(self, request, state.last_sequence)

    def stream_stats(self, key: ObservationKey) -> StreamStatistics:
        with self._condition:
            state = self._state(key)
            self._evict_expired(state, time.monotonic_ns())
            return StreamStatistics(
                key=key,
                capacity=state.capacity,
                retention_seconds=state.retention_ns / 1_000_000_000,
                frequency_hz=state.config.frequency_hz,
                buffered_items=len(state.queue),
                approximate_buffer_bytes=state.approximate_bytes,
                received_total=state.last_sequence,
                dropped_capacity=state.dropped_capacity,
                dropped_retention=state.dropped_retention,
                dropped_memory=state.dropped_memory,
            )

    def registered_streams(self) -> tuple[ObservationKey, ...]:
        with self._condition:
            return tuple(sorted(self._streams))

    @property
    def approximate_buffer_bytes(self) -> int:
        with self._condition:
            return self._approximate_bytes

    def close(self) -> None:
        with self._condition:
            self._closed = True
            for state in self._streams.values():
                state.queue.clear()
                state.approximate_bytes = 0
            self._approximate_bytes = 0
            self._condition.notify_all()

    def _derive_limits(self, config: SensorStreamConfig) -> tuple[int, int]:
        if not isinstance(config, SensorStreamConfig):
            raise TypeError("config must be SensorStreamConfig")
        retention = (config.retention_seconds if config.retention_seconds is not None
                     else self.config.default_retention_seconds)
        product = (config.capacity if config.capacity is not None
                   else float(config.frequency_hz) * float(retention))
        if not math.isfinite(product) or product > self.config.max_stream_capacity:
            raise ValueError("frequency × retention exceeds max_stream_capacity")
        count = math.ceil(product)
        if count > self.config.max_stream_capacity:
            raise ValueError("frequency × retention exceeds max_stream_capacity")
        return max(1, count), int(retention * 1_000_000_000)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ObservationPipelineError("observation pipeline is closed")

    def _state(self, key: ObservationKey) -> _StreamState:
        if not isinstance(key, ObservationKey):
            raise TypeError("key must be ObservationKey")
        try:
            return self._streams[key]
        except KeyError:
            raise ObservationPipelineError(f"unregistered observation stream: {key}") from None

    def _validate_request(self, request: ObservationRequest) -> None:
        if not isinstance(request, ObservationRequest):
            raise TypeError("request must be ObservationRequest")

    def _evict_expired(self, state: _StreamState, now_ns: int) -> None:
        cutoff = now_ns - state.retention_ns
        while state.queue and state.queue[0].received_monotonic_ns < cutoff:
            self._drop_left(state, "retention")

    def _drop_left(self, state: _StreamState, reason: str) -> None:
        record = state.queue.popleft()
        state.approximate_bytes -= record.approximate_size_bytes
        self._approximate_bytes -= record.approximate_size_bytes
        if reason == "retention":
            state.dropped_retention += 1
        elif reason == "capacity":
            state.dropped_capacity += 1
        else:
            state.dropped_memory += 1

    def _enforce_memory_budget(self, budget: int) -> None:
        while self._approximate_bytes > budget:
            candidates = (state for state in self._streams.values() if state.queue)
            oldest = min(candidates, key=lambda state: state.queue[0].received_monotonic_ns)
            self._drop_left(oldest, "memory")


class _QueueSubscription:
    """Independent cursor on a shared queue; waiting releases the writer lock."""

    def __init__(self, pipeline: QueueObservationPipeline, request: ObservationRequest,
                 starting_sequence: int) -> None:
        self._pipeline = pipeline
        self._request = request
        self._cursor = starting_sequence
        self._closed = False
        self._missed_messages = 0

    @property
    def missed_messages(self) -> int:
        with self._pipeline._condition:
            return self._missed_messages

    def read_next(self, timeout_seconds: float | None = None) -> Observation | None:
        if timeout_seconds is not None:
            if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
                raise ValueError("timeout_seconds must be a nonnegative number or None")
            if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
                raise ValueError("timeout_seconds must be a nonnegative finite number or None")
        deadline = (None if timeout_seconds is None
                    else time.monotonic() + timeout_seconds)
        pipeline = self._pipeline
        with pipeline._condition:
            while not (self._closed or pipeline._closed):
                state = pipeline._state(self._request.key)
                now_ns = time.monotonic_ns()
                pipeline._evict_expired(state, now_ns)
                for record in state.queue:
                    if record.sequence_id > self._cursor:
                        self._missed_messages += record.sequence_id - self._cursor - 1
                        self._cursor = record.sequence_id
                        if _is_stale(record, self._request, now_ns):
                            continue
                        return record
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    pipeline._condition.wait(remaining)
                else:
                    pipeline._condition.wait()
            return None

    def close(self) -> None:
        with self._pipeline._condition:
            self._closed = True
            self._pipeline._condition.notify_all()


def _estimate_size(payload: Any, explicit_bytes: int | None) -> int:
    if explicit_bytes is not None:
        return explicit_bytes
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return memoryview(payload).nbytes
    nbytes = getattr(payload, "nbytes", None)
    if isinstance(nbytes, int) and not isinstance(nbytes, bool) and nbytes >= 0:
        return nbytes
    return max(0, sys.getsizeof(payload))


def _is_stale(record: Observation, request: ObservationRequest, now_ns: int) -> bool:
    return request.max_age_ms is not None and (
        now_ns - record.received_monotonic_ns > request.max_age_ms * 1_000_000
    )


class ScopedObservationReader:
    """Read-only, allowlisted view passed to trusted registered tool adapters.

    This is an API-level restriction, not a security sandbox for malicious
    Python code in the same process. Authorization must be established by the
    trusted caller rather than by LLM-generated tool parameters.
    """

    def __init__(self, reader: ObservationReader, allowed: Iterable[ObservationKey]) -> None:
        self._reader = reader
        self._allowed = frozenset(allowed)
        if not self._allowed or not all(isinstance(key, ObservationKey) for key in self._allowed):
            raise ValueError("allowed must contain valid observation stream keys")

    def _check(self, request: ObservationRequest) -> None:
        if not isinstance(request, ObservationRequest) or request.key not in self._allowed:
            raise ObservationPipelineError("observation stream not permitted for this tool")

    def latest(self, request: ObservationRequest) -> Observation | None:
        self._check(request)
        return self._reader.latest(request)

    def window(self, request: ObservationRequest, duration_seconds: float) -> tuple[Observation, ...]:
        self._check(request)
        return self._reader.window(request, duration_seconds)

    def snapshot(self, requests: Sequence[ObservationRequest],
                 sync_tolerance_ms: float = 50.0) -> ObservationSnapshot:
        for item in requests:
            self._check(item)
        return self._reader.snapshot(requests, sync_tolerance_ms)

    def subscribe(self, request: ObservationRequest) -> ObservationSubscription:
        self._check(request)
        return self._reader.subscribe(request)
