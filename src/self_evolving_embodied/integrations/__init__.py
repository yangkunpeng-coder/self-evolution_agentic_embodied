"""Optional platform integrations; never imported by the framework core."""

from .environment import EnvironmentObservationPort, EnvironmentActionPort

__all__ = ["EnvironmentObservationPort", "EnvironmentActionPort"]
