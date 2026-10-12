"""Application-level composition for ONE LLM-centric embodied Agent.

This module wires existing planning, toolboxes, Harness and observation code.
It is not another scheduler, provider, experiment runner, or robot controller.

Trust boundaries: configuration and factories must come from trusted host code.
Model-generated data must never be used to install tool handlers, grant actions
or change policies. The physical execution bridge still requires an external
controller that really implements fencing and stop handling.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Protocol
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from threading import RLock
from typing import TypeAlias

from ..agent import (
    AgentLoopReport, AssessmentRoute, LLMSingleAgentBrain,
    SingleAgentBrain, SingleAgentExecutionLoop, TrustedToolAlternative,
)
from ..core.contracts import DomainError
from ..governance.runtime_policy import RuntimePolicyBinding
from ..harness.execution_bridge import (
    AuthorizedExecutionBridge, EffectfulToolAdapter, ExecutionGrant,
)
from ..observations import (
    Observation, ObservationInput, ObservationKey, ObservationReader, ObservationSink, PipelineConfig,
    QueueObservationPipeline, SensorStreamConfig,
)
from ..planning.llm_planner import LLMTaskPlanner, TextCompletionProvider
from ..runtime.workflow_orchestrator import EvidencePolicy, WorkflowOrchestrator
from ..storage.sqlite.database import SqliteStore
from ..toolboxes import ObservationNeeds
from ..toolboxes.assessment import (
    AssessmentToolbox, AssessorSpec, OutcomeAssessor,
)
from ..toolboxes.execution import ExecutionToolFactory, ExecutionToolbox
from ..tools.contracts import ToolSpec
from ..tools.registry import PluggableToolRegistry


BrainFactory: TypeAlias = Callable[[PluggableToolRegistry], SingleAgentBrain]


class ObservationSource(Protocol):
    """Host-provided source plugin. Its start should begin streaming to sink."""
    def start(self) -> None: ...
    def close(self) -> None: ...


ObservationSourceFactory: TypeAlias = Callable[[ObservationSink], ObservationSource]


class ManagedResource(Protocol):
    """An explicitly host-owned auxiliary resource with application lifecycle hooks.

    This is for optional model clients, network sessions and monitoring services.
    Resources do not receive execution authority or sensor Queue internals.
    """

    def start(self) -> None: ...
    def close(self) -> None: ...


ManagedResourceFactory: TypeAlias = Callable[[], ManagedResource]


class _FrameworkObservationSink:
    """Retain the framework's ingestion state check; do not expose raw queues."""
    def __init__(self, framework: "SingleAgentFramework") -> None:
        self._framework = framework

    def push(self, data: ObservationInput) -> Observation:
        return self._framework.publish_observation(data)



class FrameworkState(StrEnum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    CLOSED = "CLOSED"


@dataclass(frozen=True)
class FrameworkConfig:
    """Typed application configuration, also loadable via dev12 TOML settings.

    SQLite contains workflow and policy events, NEVER raw sensor observations.
    PipelineConfig controls bounded process-local queues (60 seconds default).
    """

    database_path: str | Path
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    max_turns: int = 40
    max_replans: int = 2

    def __post_init__(self) -> None:
        if (not isinstance(self.database_path, (str, Path))
                or not str(self.database_path).strip()):
            raise DomainError("INVALID_FRAMEWORK_DATABASE_PATH")
        if not isinstance(self.pipeline, PipelineConfig):
            raise DomainError("INVALID_FRAMEWORK_PIPELINE_CONFIG")
        if type(self.max_turns) is not int or not 1 <= self.max_turns <= 100:
            raise DomainError("INVALID_AGENT_TURN_BUDGET")
        if type(self.max_replans) is not int or not 0 <= self.max_replans <= 10:
            raise DomainError("INVALID_AGENT_REPLAN_BUDGET")


@dataclass(frozen=True)
class FrameworkComponents:
    """The actual instances used by ONE Agent; no duplicate registry or loop."""

    store: SqliteStore
    observations: QueueObservationPipeline
    registry: PluggableToolRegistry
    execution: ExecutionToolbox
    assessment: AssessmentToolbox
    brain: SingleAgentBrain
    workflow: WorkflowOrchestrator
    agent: SingleAgentExecutionLoop
    execution_bridge: AuthorizedExecutionBridge | None


@dataclass(frozen=True)
class FrameworkInventory:
    """Snapshot of installed capabilities, not a deployment or safety guarantee.

    Entries are public metadata produced by the toolboxes. No raw observations,
    secret provider settings, action grants or handler instances are exposed.
    """

    state: FrameworkState
    execution_tools: tuple[dict[str, object], ...]
    assessment_tools: tuple[dict[str, object], ...]
    bound_assessors: tuple[str, ...]
    observation_streams: tuple[ObservationKey, ...]
    managed_observation_sources: int
    managed_resources: int
    effectful_bridge_configured: bool


class SingleAgentFramework:
    """Process-local app facade around the existing event-sourced Agent loop.

    `start()` starts trusted observation-source plugins, not arbitrary ROS nodes.
    Sensor producers call `publish_observation` whenever data arrives. `run`
    invokes the single Agent synchronously and never claims semantic success
    merely because a tool/workflow reports completion.

    One task-control call at a time, but multiple sensor producers may publish
    concurrently. Optional managed observation-source plugins have start/close
    hooks; their ROS executor and physical device safety remain host concerns.
    """

    def __init__(self, config: FrameworkConfig, components: FrameworkComponents,
                 *, observation_sources: tuple[ObservationSourceFactory, ...] = (),
                 managed_resources: tuple[ManagedResourceFactory, ...] = ()) -> None:
        self.config = config
        self.components = components
        self._state = FrameworkState.CREATED
        self._lock = RLock()
        self._inflight = False
        self._source_factories = observation_sources
        self._resource_factories = managed_resources
        # Single LIFO stack preserves reverse acquisition order on all paths.
        self._active_resources: list[ManagedResource] = []

    @property
    def state(self) -> FrameworkState:
        with self._lock:
            return self._state

    @property
    def observations(self) -> ObservationReader:
        return self.components.observations

    def inventory(self) -> FrameworkInventory:
        """Inspect framework-owned capabilities without accessing private queues."""
        with self._lock:
            return FrameworkInventory(
                state=self._state,
                execution_tools=tuple(self.components.execution.catalog()),
                assessment_tools=tuple(self.components.assessment.catalog()),
                bound_assessors=tuple(self.components.assessment.bound_ids()),
                observation_streams=self.components.observations.registered_streams(),
                managed_observation_sources=len(self._source_factories),
                managed_resources=len(self._resource_factories),
                effectful_bridge_configured=self.components.execution_bridge is not None,
            )

    def start(self) -> "SingleAgentFramework":
        """Start host-managed dependencies and observation sources with rollback.

        Managed dependencies start first, observation sources second. STARTING
        permits source callbacks to push observations, but not Agent tasks.
        Cleanup is reverse order; it is not realtime ROS executor control.
        """
        with self._lock:
            if self._state != FrameworkState.CREATED:
                raise DomainError("FRAMEWORK_NOT_STARTABLE")
            self._state = FrameworkState.STARTING
        try:
            # Dependencies start before sensor sources and shut down after them.
            for factory in self._resource_factories:
                resource = factory()
                if (not callable(getattr(resource, "start", None))
                        or not callable(getattr(resource, "close", None))):
                    raise DomainError("INVALID_MANAGED_RESOURCE")
                self._active_resources.append(resource)
                resource.start()
            sink = _FrameworkObservationSink(self)
            for factory in self._source_factories:
                resource = factory(sink)
                if (not callable(getattr(resource, "start", None))
                        or not callable(getattr(resource, "close", None))):
                    raise DomainError("INVALID_OBSERVATION_SOURCE")
                # Track before calling start, so a partially started plugin can
                # still be closed during rollback.
                self._active_resources.append(resource)
                resource.start()
            with self._lock:
                self._state = FrameworkState.RUNNING
            return self
        except BaseException as startup_error:
            # Do not hide the startup failure if cleanup also fails.
            shutdown_error = self._stop_resources()
            self.components.observations.close()
            with self._lock:
                self._state = FrameworkState.CLOSED
            if shutdown_error is not None:
                startup_error.add_note("An additional managed resource cleanup failure occurred")
            raise

    def _stop_resources(self) -> Exception | None:
        """Best-effort LIFO cleanup of all sources and explicitly owned resources."""
        first_error: Exception | None = None
        while self._active_resources:
            resource = self._active_resources.pop()
            try:
                resource.close()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        return first_error

    def publish_observation(self, record: ObservationInput) -> Observation:
        """Non-persistent ingestion; safe to call from ROS callback threads."""
        with self._lock:
            if self._state not in (FrameworkState.STARTING, FrameworkState.RUNNING):
                raise DomainError("FRAMEWORK_NOT_RUNNING")
            return self.components.observations.push(record)

    def run_instruction(self, instruction: str, *, run_id: str | None = None) -> AgentLoopReport:
        """Natural-language entry point; LLM Brain plans and reviews every step."""
        self._begin_agent_operation()
        try:
            return self.components.agent.start(
                instruction, run_id=run_id,
                max_turns=self.config.max_turns,
                max_replans=self.config.max_replans,
            )
        finally:
            self._end_agent_operation()

    def resume_instruction(self, root_run_id: str, *,
                           retry_observation: bool = False) -> AgentLoopReport:
        """Use existing durable workflow state; no automatic replay of unknown actions."""
        self._begin_agent_operation()
        try:
            return self.components.agent.resume(
                root_run_id,
                max_turns=self.config.max_turns,
                max_replans=self.config.max_replans,
                retry_observation=retry_observation,
            )
        finally:
            self._end_agent_operation()

    def _require_running(self) -> None:
        if self._state != FrameworkState.RUNNING:
            raise DomainError("FRAMEWORK_NOT_RUNNING")

    def _begin_agent_operation(self) -> None:
        with self._lock:
            self._require_running()
            if self._inflight:
                raise DomainError("SINGLE_AGENT_OPERATION_ALREADY_ACTIVE")
            self._inflight = True

    def _end_agent_operation(self) -> None:
        with self._lock:
            self._inflight = False

    def close(self) -> None:
        with self._lock:
            if self._state == FrameworkState.CLOSED:
                return
            if self._state in (FrameworkState.STARTING, FrameworkState.STOPPING):
                raise DomainError("FRAMEWORK_LIFECYCLE_BUSY")
            if self._inflight:
                # Synchronous external calls cannot safely be cancelled by close.
                raise DomainError("AGENT_OPERATION_ACTIVE")
            self._state = FrameworkState.STOPPING
        error = self._stop_resources()
        try:
            self.components.observations.close()
        finally:
            with self._lock:
                self._state = FrameworkState.CLOSED
        if error is not None:
            raise DomainError("FRAMEWORK_RESOURCE_SHUTDOWN_FAILED") from error

    def __enter__(self) -> "SingleAgentFramework":
        return self.start()

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        try:
            self.close()
        except Exception:
            if not isinstance(_exc, BaseException):
                raise
            # Preserve the user's original exception; cleanup errors are still
            # reported as an exception note, not silently converted to success.
            _exc.add_note("Framework shutdown also raised an exception")


class FrameworkBuilder:
    """Explicit trusted-host composition root; build exactly once.

    Plugin code receives scoped ObservationReader through existing toolboxes.
    Only trusted host Python code supplies factories, assessor implementations,
    evidence policies and effectful ExecutionGrants. No dynamic imports from
    LLM instructions or untrusted TOML are performed.
    """

    def __init__(self, config: FrameworkConfig, *,
                 provider: TextCompletionProvider | None = None,
                 brain_factory: BrainFactory | None = None) -> None:
        if not isinstance(config, FrameworkConfig):
            raise DomainError("INVALID_FRAMEWORK_CONFIG")
        if (provider is None) == (brain_factory is None):
            raise DomainError("FRAMEWORK_BRAIN_PROVIDER_REQUIRED")
        if provider is not None and not callable(getattr(provider, "complete", None)):
            raise DomainError("INVALID_LLM_PROVIDER")
        if brain_factory is not None and not callable(brain_factory):
            raise DomainError("INVALID_BRAIN_FACTORY")
        self.config = config
        self._provider = provider
        self._brain_factory = brain_factory
        self._streams: list[SensorStreamConfig] = []
        self._execution: list[tuple[ToolSpec, ObservationNeeds, ExecutionToolFactory | None]] = []
        self._assessors: list[tuple[AssessorSpec, OutcomeAssessor | None]] = []
        self._routes: list[AssessmentRoute] = []
        self._alternatives: list[TrustedToolAlternative] = []
        self._evidence: dict[str, EvidencePolicy] = {}
        self._effectful: list[tuple[ExecutionGrant, EffectfulToolAdapter]] = []
        self._sources: list[ObservationSourceFactory] = []
        self._resources: list[ManagedResourceFactory] = []
        self._runtime_policy: RuntimePolicyBinding | None = None
        self._built = False

    def _mutable(self) -> None:
        if self._built:
            raise DomainError("FRAMEWORK_BUILDER_ALREADY_USED")

    def add_stream(self, config: SensorStreamConfig) -> "FrameworkBuilder":
        self._mutable()
        if not isinstance(config, SensorStreamConfig):
            raise DomainError("INVALID_SENSOR_STREAM_CONFIG")
        self._streams.append(config)
        return self

    def add_execution_tool(self, spec: ToolSpec, *,
                           needs: ObservationNeeds | None = None,
                           factory: ExecutionToolFactory | None = None) -> "FrameworkBuilder":
        self._mutable()
        if not isinstance(spec, ToolSpec):
            raise DomainError("INVALID_TOOL_SPEC")
        if factory is not None and not callable(factory):
            raise DomainError("INVALID_TOOL_FACTORY")
        requirement = needs if needs is not None else ObservationNeeds()
        if not isinstance(requirement, ObservationNeeds):
            raise DomainError("INVALID_OBSERVATION_NEEDS")
        self._execution.append((spec, requirement, factory))
        return self

    def add_assessor(self, spec: AssessorSpec,
                     handler: OutcomeAssessor | None = None) -> "FrameworkBuilder":
        self._mutable()
        if not isinstance(spec, AssessorSpec):
            raise DomainError("INVALID_ASSESSOR_SPEC")
        if handler is not None and not callable(getattr(handler, "assess", None)):
            raise DomainError("INVALID_ASSESSOR_HANDLER")
        self._assessors.append((spec, handler))
        return self

    def add_assessment_route(self, route: AssessmentRoute) -> "FrameworkBuilder":
        self._mutable()
        if not isinstance(route, AssessmentRoute):
            raise DomainError("INVALID_ASSESSMENT_ROUTE")
        self._routes.append(route)
        return self

    def add_trusted_alternative(self, alternative: TrustedToolAlternative) -> "FrameworkBuilder":
        self._mutable()
        if not isinstance(alternative, TrustedToolAlternative):
            raise DomainError("INVALID_TOOL_ALTERNATIVE")
        self._alternatives.append(alternative)
        return self

    def add_evidence_policy(self, condition: str,
                            policy: EvidencePolicy) -> "FrameworkBuilder":
        self._mutable()
        if not isinstance(condition, str) or not condition.strip() or not callable(policy):
            raise DomainError("INVALID_EVIDENCE_POLICY")
        if condition in self._evidence:
            raise DomainError("DUPLICATE_EVIDENCE_POLICY")
        self._evidence[condition] = policy
        return self

    def bind_effectful(self, grant: ExecutionGrant,
                       adapter: EffectfulToolAdapter) -> "FrameworkBuilder":
        """Trusted host only. Registration is not physical-action authorization."""
        self._mutable()
        if not isinstance(grant, ExecutionGrant):
            raise DomainError("INVALID_EXECUTION_GRANT")
        self._effectful.append((grant, adapter))
        return self

    def add_observation_source(self, factory: ObservationSourceFactory) -> "FrameworkBuilder":
        """Install a trusted, managed source; no ROS/SDK dependency is imported."""
        self._mutable()
        if not callable(factory):
            raise DomainError("INVALID_OBSERVATION_SOURCE_FACTORY")
        self._sources.append(factory)
        return self

    def add_managed_resource(self, factory: ManagedResourceFactory) -> "FrameworkBuilder":
        """Opt-in host-owned lifecycle; never inferred from a provider object.

        For network clients and other host dependencies requiring deterministic
        cleanup. Source plugins continue to use add_observation_source(sink).
        """
        self._mutable()
        if not callable(factory):
            raise DomainError("INVALID_MANAGED_RESOURCE_FACTORY")
        self._resources.append(factory)
        return self

    def bind_runtime_policy(self, policy: RuntimePolicyBinding) -> "FrameworkBuilder":
        self._mutable()
        if not isinstance(policy, RuntimePolicyBinding):
            raise DomainError("INVALID_RUNTIME_POLICY_BINDING")
        self._runtime_policy = policy
        return self

    def build(self) -> SingleAgentFramework:
        """Construct ONE shared tool registry and ONE Agent execution loop.

        A failed composition closes its in-memory Pipeline. Trusted plugin
        factories may perform their own initialization; this method does not
        dispatch agent tasks or push sensor observations. Model/robot actions
        must still pass through their normal registered execution boundary.
        """
        self._mutable()
        self._built = True
        pipeline = QueueObservationPipeline(self.config.pipeline)
        try:
            for stream in self._streams:
                pipeline.register_stream(stream)
            # Resolve observation dependencies before invoking any tool/model
            # factories. A misspelled source ID must fail at composition time.
            available = set(pipeline.registered_streams())
            for _, needs, _ in self._execution:
                if not set(needs.keys).issubset(available):
                    raise DomainError("UNREGISTERED_TOOL_OBSERVATION_SOURCE")
            for spec, _ in self._assessors:
                if not set(spec.observation_needs.keys).issubset(available):
                    raise DomainError("UNREGISTERED_ASSESSOR_OBSERVATION_SOURCE")
            registry = PluggableToolRegistry()
            execution = ExecutionToolbox(registry, observations=pipeline)
            for spec, needs, factory in self._execution:
                execution.register(spec, needs=needs, factory=factory)
            assessments = AssessmentToolbox(observations=pipeline)
            for spec, handler in self._assessors:
                assessments.register(spec, handler)
            bridge = None
            if self._effectful:
                bridge = AuthorizedExecutionBridge(registry)
                for grant, adapter in self._effectful:
                    bridge.bind(grant, adapter)
            if self._provider is not None:
                planner = LLMTaskPlanner(self._provider, registry)
                brain: SingleAgentBrain = LLMSingleAgentBrain(self._provider, planner)
            else:
                if self._brain_factory is None:
                    raise DomainError("FRAMEWORK_BRAIN_PROVIDER_REQUIRED")
                brain = self._brain_factory(registry)
                if not all(callable(getattr(brain, member, None)) for member in
                           ("plan", "decide", "replan")):
                    raise DomainError("INVALID_BRAIN_IMPLEMENTATION")
            store = SqliteStore(self.config.database_path)
            workflow = WorkflowOrchestrator(
                store, registry, self._evidence,
                execution_bridge=bridge, require_agent_feedback=True,
            )
            agent = SingleAgentExecutionLoop(
                brain, workflow, assessments,
                routes=tuple(self._routes),
                runtime_policy=self._runtime_policy,
                tool_alternatives=tuple(self._alternatives),
                execution_toolbox=execution, observations=pipeline,
            )
            return SingleAgentFramework(
                self.config,
                FrameworkComponents(store, pipeline, registry, execution,
                                    assessments, brain, workflow, agent, bridge),
                observation_sources=tuple(self._sources),
                managed_resources=tuple(self._resources),
            )
        except BaseException:
            pipeline.close()
            raise
