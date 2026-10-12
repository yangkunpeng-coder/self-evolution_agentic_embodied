"""Environment-neutral ports backed by the existing observation and Harness contracts.

These are *interfaces*, not a second execution dispatcher or a sensor bus.
"""
from __future__ import annotations

from typing import Protocol

from ...harness.execution_bridge import EffectfulToolAdapter
from ...observations import ObservationSink


class EnvironmentObservationPort(ObservationSink, Protocol):
    """A source pushes independently timestamped data into Observation Pipeline."""


class EnvironmentActionPort(EffectfulToolAdapter, Protocol):
    """Physically effectful capabilities cross the existing authorized bridge."""


__all__ = ["EnvironmentObservationPort", "EnvironmentActionPort"]
