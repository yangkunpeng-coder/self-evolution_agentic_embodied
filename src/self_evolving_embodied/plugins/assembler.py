"""Strict declarative selection -> dev11 FrameworkBuilder composition.

Only factories previously registered by trusted host code may run. The TOML
file grants no physical action authority, cannot load arbitrary modules, and
cannot modify Harness evidence policies or released governance policies.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

from ..agent import AssessmentRoute
from ..application import FrameworkBuilder, SingleAgentFramework
from ..core.contracts import DomainError
from ..runtime.workflow_orchestrator import EvidencePolicy
from .catalog import PluginKind, TrustedPluginCatalog
from .configuration import FrameworkSettings


def build_configured_framework(settings: FrameworkSettings,
                               catalog: TrustedPluginCatalog, *,
                               evidence_policies: Mapping[str, EvidencePolicy] | None = None,
                               assessment_routes: Sequence[AssessmentRoute] = ()) -> SingleAgentFramework:
    """Compose selected plugins with explicit trusted-host verification/routing.

    TOML selects capabilities; it cannot supply executable evidence policies or
    grants. Missing verifiers still pause safely at WAITING_VERIFIER.
    """
    if not isinstance(settings, FrameworkSettings) or not isinstance(catalog, TrustedPluginCatalog):
        raise DomainError("INVALID_FRAMEWORK_PLUGIN_CONFIG")

    # Resolve every ID and version before constructing any plugin or touching SQLite.
    brain = catalog.resolve(settings.brain,
                            (PluginKind.LLM_PROVIDER, PluginKind.BRAIN_FACTORY))
    execution = [catalog.resolve(s, PluginKind.EXECUTION) for s in settings.execution]
    assessment = [catalog.resolve(s, PluginKind.ASSESSMENT) for s in settings.assessment]
    sources = [catalog.resolve(s, PluginKind.OBSERVATION_SOURCE) for s in settings.sources]
    catalog.seal()

    if brain.manifest.kind is PluginKind.LLM_PROVIDER:
        if brain.factory is None:
            raise DomainError("PLUGIN_FACTORY_MISSING")
        provider = brain.factory()
        builder = FrameworkBuilder(settings.framework, provider=provider)
    else:
        if brain.factory is None:
            raise DomainError("PLUGIN_FACTORY_MISSING")
        builder = FrameworkBuilder(settings.framework, brain_factory=brain.factory)
    for stream in settings.streams:
        builder.add_stream(stream)
    for entry in execution:
        if entry.tool_spec is None or entry.observation_needs is None:
            raise DomainError("INCOMPLETE_EXECUTION_PLUGIN")
        builder.add_execution_tool(entry.tool_spec, needs=entry.observation_needs,
                                   factory=entry.factory)
    for entry in assessment:
        if entry.assessor_spec is None:
            raise DomainError("INCOMPLETE_ASSESSMENT_PLUGIN")
        handler = entry.factory() if entry.factory is not None else None
        builder.add_assessor(entry.assessor_spec, handler)
    for entry in sources:
        if entry.factory is None:
            raise DomainError("PLUGIN_FACTORY_MISSING")
        builder.add_observation_source(entry.factory)
    for condition, policy in (evidence_policies or {}).items():
        builder.add_evidence_policy(condition, policy)
    for route in assessment_routes:
        builder.add_assessment_route(route)
    return builder.build()
