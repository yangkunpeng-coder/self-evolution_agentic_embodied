"""Execution toolbox facade over the existing authorized tool infrastructure.

READ_ONLY providers bind into PluggableToolRegistry and hence into the
existing WorkflowOrchestrator. Effectful tools remain declarations until a
trusted AuthorizedExecutionBridge is explicitly configured by the host.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ...core.contracts import DomainError
from ...observations import ObservationReader
from ...tools.contracts import Effect, ToolHandler, ToolSpec
from ...tools.registry import PluggableToolRegistry
from ..common import ObservationNeeds, make_reader, unavailable_required

ExecutionToolFactory = Callable[[ObservationReader | None], ToolHandler]


@dataclass(frozen=True)
class ExecutionToolEntry:
    spec: ToolSpec
    observation_needs: ObservationNeeds
    bound_read_only: bool


class _ReadOnlyObservationHandler:
    def __init__(self, handler: ToolHandler, reader: ObservationReader | None,
                 needs: ObservationNeeds) -> None:
        self._handler = handler
        self._reader = reader
        self._needs = needs

    def invoke(self, arguments):
        unavailable = unavailable_required(self._reader, self._needs)
        if unavailable:
            raise DomainError("REQUIRED_OBSERVATION_UNAVAILABLE")
        return self._handler.invoke(arguments)


class ExecutionToolbox:
    """Single Agent execution capabilities; NOT a second tool dispatcher.

    Physical actions are never invoked by this class. Trusted host code must
    separately bind an EffectfulToolAdapter with an ExecutionGrant to the
    AuthorizedExecutionBridge. Neither LLM outputs nor observation metadata
    confer that authority.
    """

    def __init__(self, registry: PluggableToolRegistry,
                 observations: ObservationReader | None = None) -> None:
        if not isinstance(registry, PluggableToolRegistry):
            raise TypeError("registry must be PluggableToolRegistry")
        self.registry = registry
        self._observations = observations
        self._needs: dict[str, ObservationNeeds] = {}

    def register(self, spec: ToolSpec, *, needs: ObservationNeeds | None = None,
                 factory: ExecutionToolFactory | None = None) -> ExecutionToolEntry:
        """Declare a tool; optionally bind a trusted READ_ONLY implementation."""
        if not isinstance(spec, ToolSpec):
            raise DomainError("INVALID_TOOL_SPEC")
        requirement = needs if needs is not None else ObservationNeeds()
        if not isinstance(requirement, ObservationNeeds):
            raise DomainError("INVALID_OBSERVATION_NEEDS")
        if factory is not None and spec.effect is not Effect.READ_ONLY:
            raise DomainError("EFFECTFUL_TOOL_REQUIRES_HARNESS")
        if factory is not None and not callable(factory):
            raise DomainError("INVALID_TOOL_FACTORY")
        reader = make_reader(self._observations, requirement) if factory is not None else None
        handler = self._make_handler(factory, reader, requirement) if factory else None
        self.registry.register(spec, handler)
        self._needs[spec.tool_id] = requirement
        return self.resolve(spec.tool_id)

    def adopt(self, tool_id: str, *, needs: ObservationNeeds | None = None) -> ExecutionToolEntry:
        """Add observation declarations for an existing dev0/dev3 catalog entry."""
        self.registry.resolve(tool_id)
        if tool_id in self._needs:
            raise DomainError("TOOL_ALREADY_ADOPTED")
        requirement = needs if needs is not None else ObservationNeeds()
        if not isinstance(requirement, ObservationNeeds):
            raise DomainError("INVALID_OBSERVATION_NEEDS")
        self._needs[tool_id] = requirement
        return self.resolve(tool_id)

    def bind_read_only(self, tool_id: str, factory: ExecutionToolFactory) -> None:
        spec = self.registry.resolve(tool_id)
        if spec.effect is not Effect.READ_ONLY:
            raise DomainError("EFFECTFUL_TOOL_REQUIRES_HARNESS")
        if tool_id not in self._needs:
            raise DomainError("TOOL_NOT_ADOPTED")
        if not callable(factory):
            raise DomainError("INVALID_TOOL_FACTORY")
        requirement = self._needs[tool_id]
        reader = make_reader(self._observations, requirement)
        handler = self._make_handler(factory, reader, requirement)
        self.registry.bind(tool_id, handler)

    @staticmethod
    def _make_handler(factory: ExecutionToolFactory, reader: ObservationReader | None,
                      needs: ObservationNeeds) -> ToolHandler:
        handler = factory(reader)
        if not callable(getattr(handler, "invoke", None)):
            raise DomainError("INVALID_TOOL_HANDLER")
        return _ReadOnlyObservationHandler(handler, reader, needs)

    def resolve(self, tool_id: str) -> ExecutionToolEntry:
        spec = self.registry.resolve(tool_id)
        if tool_id not in self._needs:
            raise DomainError("TOOL_NOT_ADOPTED")
        return ExecutionToolEntry(spec, self._needs[tool_id], self.registry.is_bound(tool_id))

    def catalog(self) -> list[dict[str, object]]:
        return [{**self.registry.resolve(tool_id).public_description(),
                 "observation_needs": self._needs[tool_id].public_description(),
                 "is_bound_read_only": self.registry.is_bound(tool_id)}
                for tool_id in sorted(self._needs)]

    def scoped_observations(self, tool_id: str) -> ObservationReader | None:
        """Give a trusted effectful adapter scoped observation access at binding time.

        This does NOT authorize invocation; execution still requires a grant.
        """
        return make_reader(self._observations, self.resolve(tool_id).observation_needs)
