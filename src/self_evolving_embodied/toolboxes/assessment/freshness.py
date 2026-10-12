"""Read-only assessment view enforcing a maximum data age per observation key."""
from __future__ import annotations
from typing import Sequence
from ...observations import ObservationReader, ObservationRequest


class FreshnessBoundReader:
    def __init__(self, delegate: ObservationReader, max_age_ms: int) -> None:
        if type(max_age_ms) is not int or max_age_ms <= 0:
            raise ValueError("max_age_ms must be positive")
        self._delegate = delegate
        self._limit = max_age_ms

    def _clamp(self, request: ObservationRequest) -> ObservationRequest:
        ceiling = self._limit if request.max_age_ms is None else min(self._limit, request.max_age_ms)
        return ObservationRequest(request.key, max_age_ms=ceiling)

    def latest(self, request: ObservationRequest):
        return self._delegate.latest(self._clamp(request))

    def window(self, request: ObservationRequest, duration_seconds: float):
        return self._delegate.window(self._clamp(request), duration_seconds)

    def snapshot(self, requests: Sequence[ObservationRequest], sync_tolerance_ms: float = 50.0):
        return self._delegate.snapshot(tuple(self._clamp(x) for x in requests),
                                       sync_tolerance_ms=sync_tolerance_ms)

    def subscribe(self, request: ObservationRequest):
        return self._delegate.subscribe(self._clamp(request))
