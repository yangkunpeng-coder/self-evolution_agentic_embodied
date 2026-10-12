"""Shared contracts for the two logically separate toolboxes.

The toolbox layer consumes ObservationReader only; it neither owns sensor
queues nor knows ROS messages. Trusted code declares observation permissions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..core.contracts import DomainError
from ..observations import (
    Observation, ObservationKey, ObservationReader, ObservationRequest,
    ObservationSnapshot, ObservationSubscription, ScopedObservationReader,
)


@dataclass(frozen=True)
class ObservationNeeds:
    """Static per-plugin data access declaration, NOT model-supplied arguments."""

    required: tuple[ObservationRequest, ...] = ()
    optional: tuple[ObservationRequest, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.required, tuple) or not isinstance(self.optional, tuple):
            raise DomainError("INVALID_OBSERVATION_NEEDS")
        items = self.required + self.optional
        if not all(isinstance(item, ObservationRequest) for item in items):
            raise DomainError("INVALID_OBSERVATION_NEEDS")
        if len(items) > 32 or len({req.key for req in items}) != len(items):
            raise DomainError("INVALID_OBSERVATION_NEEDS")

    @property
    def keys(self) -> tuple[ObservationKey, ...]:
        return tuple(req.key for req in self.required + self.optional)

    def public_description(self) -> dict[str, object]:
        def describe(req: ObservationRequest) -> dict[str, object]:
            return {"source_id": req.key.source_id, "modality": req.key.modality,
                    "max_age_ms": req.max_age_ms}
        return {"required": [describe(req) for req in self.required],
                "optional": [describe(req) for req in self.optional]}


class ToolboxObservationReader:
    """Restrict sensor keys and never weaken a declared freshness ceiling.

    This is an in-process API restriction, NOT isolation against malicious
    plugin Python code. A real trust boundary requires process isolation.
    """

    def __init__(self, reader: ObservationReader, needs: ObservationNeeds) -> None:
        if not needs.keys:
            raise DomainError("OBSERVATION_NEEDS_EMPTY")
        self._reader = ScopedObservationReader(reader, needs.keys)
        self._limits = {req.key: req.max_age_ms for req in needs.required + needs.optional}

    def _request(self, req: ObservationRequest) -> ObservationRequest:
        if not isinstance(req, ObservationRequest) or req.key not in self._limits:
            raise DomainError("OBSERVATION_ACCESS_DENIED")
        ceiling = self._limits[req.key]
        if ceiling is None:
            return req
        return ObservationRequest(req.key, max_age_ms=(
            ceiling if req.max_age_ms is None else min(ceiling, req.max_age_ms)
        ))

    def latest(self, request: ObservationRequest) -> Observation | None:
        return self._reader.latest(self._request(request))

    def window(self, request: ObservationRequest, duration_seconds: float) -> tuple[Observation, ...]:
        return self._reader.window(self._request(request), duration_seconds)

    def snapshot(self, requests: Sequence[ObservationRequest],
                 sync_tolerance_ms: float = 50.0) -> ObservationSnapshot:
        return self._reader.snapshot(tuple(self._request(req) for req in requests),
                                     sync_tolerance_ms=sync_tolerance_ms)

    def subscribe(self, request: ObservationRequest) -> ObservationSubscription:
        return self._reader.subscribe(self._request(request))


def make_reader(reader: ObservationReader | None,
                needs: ObservationNeeds) -> ObservationReader | None:
    if not needs.keys:
        return None
    if reader is None:
        raise DomainError("OBSERVATION_PIPELINE_REQUIRED")
    return ToolboxObservationReader(reader, needs)


def unavailable_required(reader: ObservationReader | None,
                         needs: ObservationNeeds) -> tuple[ObservationKey, ...]:
    """A read-only preflight. Does not pretend missing observations are success."""
    if not needs.required:
        return ()
    if reader is None:
        return tuple(req.key for req in needs.required)
    missing: list[ObservationKey] = []
    for req in needs.required:
        try:
            if reader.latest(req) is None:
                missing.append(req.key)
        except (KeyError, LookupError, RuntimeError, ValueError):
            missing.append(req.key)
    return tuple(missing)
