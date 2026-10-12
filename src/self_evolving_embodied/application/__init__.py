"""Unified composition entry point for the single embodied Agent."""
from .core import (
    BrainFactory, ObservationSource, ObservationSourceFactory,
    ManagedResource, ManagedResourceFactory, FrameworkBuilder, FrameworkComponents,
    FrameworkConfig, FrameworkInventory, FrameworkState, SingleAgentFramework,
)

__all__ = [
    "BrainFactory", "ObservationSource", "ObservationSourceFactory",
    "ManagedResource", "ManagedResourceFactory", "FrameworkBuilder", "FrameworkComponents",
    "FrameworkConfig", "FrameworkInventory", "FrameworkState", "SingleAgentFramework",
]
