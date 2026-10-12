"""Trusted-host plugin catalog: never import implementation paths from TOML/LLM.

The catalog is an application composition facility, not a sandbox. Only trusted
host code may register factories. Selection pins a plugin's exact version.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ..core.contracts import DomainError
from ..observations import ObservationSink
from ..toolboxes import ObservationNeeds
from ..tools.contracts import Effect, ToolSpec
from ..toolboxes.assessment import AssessorSpec
from ..toolboxes.execution import ExecutionToolFactory

_ID = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._+-]{0,39}$")


class PluginKind(StrEnum):
    LLM_PROVIDER = "LLM_PROVIDER"
    BRAIN_FACTORY = "BRAIN_FACTORY"
    EXECUTION = "EXECUTION"
    ASSESSMENT = "ASSESSMENT"
    OBSERVATION_SOURCE = "OBSERVATION_SOURCE"


@dataclass(frozen=True)
class PluginManifest:
    plugin_id: str
    version: str
    kind: PluginKind
    description: str

    def __post_init__(self) -> None:
        if not isinstance(self.plugin_id, str) or not _ID.fullmatch(self.plugin_id):
            raise DomainError("INVALID_PLUGIN_ID")
        if not isinstance(self.version, str) or not _VERSION.fullmatch(self.version):
            raise DomainError("INVALID_PLUGIN_VERSION")
        if not isinstance(self.kind, PluginKind):
            raise DomainError("INVALID_PLUGIN_KIND")
        if not isinstance(self.description, str) or not 1 <= len(self.description.strip()) <= 300:
            raise DomainError("INVALID_PLUGIN_DESCRIPTION")


@dataclass(frozen=True)
class PluginSelection:
    plugin_id: str
    version: str

    def __post_init__(self) -> None:
        if not isinstance(self.plugin_id, str) or not _ID.fullmatch(self.plugin_id):
            raise DomainError("INVALID_PLUGIN_ID")
        if not isinstance(self.version, str) or not _VERSION.fullmatch(self.version):
            raise DomainError("INVALID_PLUGIN_VERSION")


@dataclass(frozen=True)
class PluginEntry:
    manifest: PluginManifest
    factory: Callable[..., Any] | None
    tool_spec: ToolSpec | None = None
    observation_needs: ObservationNeeds | None = None
    assessor_spec: AssessorSpec | None = None


class TrustedPluginCatalog:
    """Allowlist assembled in trusted Python; IDs select pre-installed factories.

    Registration is independent of execution authorization. In particular an
    effectful ToolSpec may be declared but cannot be bound to an execution
    handler from this catalog; a separate trusted ExecutionGrant is required.
    """

    def __init__(self) -> None:
        self._entries: dict[str, PluginEntry] = {}
        self._sealed = False

    def _put(self, manifest: PluginManifest, factory: Callable[..., Any] | None,
             *, spec: ToolSpec | None = None, needs: ObservationNeeds | None = None,
             assessor: AssessorSpec | None = None) -> None:
        if self._sealed:
            raise DomainError("PLUGIN_CATALOG_SEALED")
        if not isinstance(manifest, PluginManifest):
            raise DomainError("INVALID_PLUGIN_MANIFEST")
        if manifest.plugin_id in self._entries:
            raise DomainError("DUPLICATE_PLUGIN_ID")
        if factory is not None and not callable(factory):
            raise DomainError("INVALID_PLUGIN_FACTORY")
        self._entries[manifest.plugin_id] = PluginEntry(manifest, factory, spec, needs, assessor)

    def register_llm_provider(self, manifest: PluginManifest,
                              factory: Callable[[], Any]) -> None:
        if manifest.kind is not PluginKind.LLM_PROVIDER or not callable(factory):
            raise DomainError("PLUGIN_KIND_MISMATCH")
        self._put(manifest, factory)

    def register_brain_factory(self, manifest: PluginManifest,
                               factory: Callable[[Any], Any]) -> None:
        if manifest.kind is not PluginKind.BRAIN_FACTORY or not callable(factory):
            raise DomainError("PLUGIN_KIND_MISMATCH")
        self._put(manifest, factory)

    def register_execution(self, manifest: PluginManifest, spec: ToolSpec,
                           *, needs: ObservationNeeds | None = None,
                           factory: ExecutionToolFactory | None = None) -> None:
        if manifest.kind is not PluginKind.EXECUTION or not isinstance(spec, ToolSpec):
            raise DomainError("PLUGIN_KIND_MISMATCH")
        if manifest.version != spec.version:
            raise DomainError("PLUGIN_TOOL_VERSION_MISMATCH")
        if factory is not None and spec.effect is not Effect.READ_ONLY:
            raise DomainError("EFFECTFUL_TOOL_REQUIRES_HARNESS")
        requirement = needs if needs is not None else ObservationNeeds()
        if not isinstance(requirement, ObservationNeeds):
            raise DomainError("INVALID_OBSERVATION_NEEDS")
        self._put(manifest, factory, spec=spec, needs=requirement)

    def register_assessor(self, manifest: PluginManifest, spec: AssessorSpec,
                          *, factory: Callable[[], Any] | None = None) -> None:
        if manifest.kind is not PluginKind.ASSESSMENT or not isinstance(spec, AssessorSpec):
            raise DomainError("PLUGIN_KIND_MISMATCH")
        if manifest.version != spec.version:
            raise DomainError("PLUGIN_ASSESSOR_VERSION_MISMATCH")
        self._put(manifest, factory, assessor=spec)

    def register_observation_source(self, manifest: PluginManifest,
                                    factory: Callable[[ObservationSink], Any]) -> None:
        if manifest.kind is not PluginKind.OBSERVATION_SOURCE or not callable(factory):
            raise DomainError("PLUGIN_KIND_MISMATCH")
        self._put(manifest, factory)

    def resolve(self, selection: PluginSelection,
                expected: PluginKind | tuple[PluginKind, ...]) -> PluginEntry:
        if not isinstance(selection, PluginSelection):
            raise DomainError("INVALID_PLUGIN_SELECTION")
        if not isinstance(expected, tuple):
            expected = (expected,)
        entry = self._entries.get(selection.plugin_id)
        if entry is None:
            raise DomainError("PLUGIN_NOT_ALLOWLISTED")
        if entry.manifest.kind not in expected:
            raise DomainError("PLUGIN_KIND_MISMATCH")
        if entry.manifest.version != selection.version:
            raise DomainError("PLUGIN_VERSION_MISMATCH")
        return entry

    def seal(self) -> None:
        self._sealed = True

    def catalog(self) -> tuple[PluginManifest, ...]:
        return tuple(self._entries[k].manifest for k in sorted(self._entries))
