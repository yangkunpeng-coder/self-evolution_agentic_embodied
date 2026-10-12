"""Stable public imports for the single-Agent embodied research framework.

This package is a platform-independent framework; optional models and robots
are provided by explicitly trusted host adapters. See the framework README
and examples/framework_minimal.py for composition and public API usage.
"""

__version__ = "1.1.0rc3"

from .application import (
    FrameworkBuilder, FrameworkConfig, FrameworkInventory,
    FrameworkState, SingleAgentFramework,
)
from .plugins import (
    PluginKind, PluginManifest, PluginSelection, TrustedPluginCatalog,
    FrameworkSettings, load_framework_settings, build_configured_framework,
)
from .observations import (
    Observation, ObservationInput, ObservationKey, ObservationReader,
    ObservationRequest, ObservationSink, ObservationSnapshot, ObservationSubscription,
    PipelineConfig, QueueObservationPipeline, SensorStreamConfig,
)
from .toolboxes import ObservationNeeds, ExecutionToolbox, AssessmentToolbox
from .toolboxes.assessment import (
    AssessmentRequest, AssessmentResult, AssessorSpec, Outcome,
)
from .tools.contracts import Effect, ToolKind, ToolSpec
from .agent import (
    AgentAction, AgentDecision, AgentLoopReport, AssessmentRoute, BrainContext, SingleAgentBrain,
)
from .planning.llm_planner import TextCompletionProvider
from .harness.execution_bridge import ExecutionGrant, EffectfulToolAdapter
from .core.contracts import DomainError

__all__ = [
    "__version__", "FrameworkBuilder", "FrameworkConfig", "FrameworkInventory",
    "FrameworkState", "SingleAgentFramework", "DomainError",
    "PluginKind", "PluginManifest", "PluginSelection", "TrustedPluginCatalog",
    "FrameworkSettings", "load_framework_settings", "build_configured_framework",
    "Observation", "ObservationInput", "ObservationKey", "ObservationReader",
    "ObservationRequest", "ObservationSink", "ObservationSnapshot", "ObservationSubscription",
    "PipelineConfig", "QueueObservationPipeline", "SensorStreamConfig",
    "ObservationNeeds", "ExecutionToolbox", "AssessmentToolbox",
    "AssessmentRequest", "AssessmentResult", "AssessorSpec", "Outcome",
    "Effect", "ToolKind", "ToolSpec", "AgentLoopReport", "AssessmentRoute",
    "AgentAction", "AgentDecision", "BrainContext", "SingleAgentBrain", "TextCompletionProvider",
    "ExecutionGrant", "EffectfulToolAdapter",
]
