"""Dependency scheduler and durable high-level workflow facade."""
from __future__ import annotations

from collections.abc import Mapping

from ..core.contracts import DomainError
from ..harness.execution_bridge import AuthorizedExecutionBridge
from ..planning.contracts import Subtask, TaskPlan
from ..planning.validator import PlanValidator
from ..storage.sqlite.database import SqliteStore
from ..tools.registry import PluggableToolRegistry
from .workflow_orchestrator import EvidencePolicy, WorkflowOrchestrator
from .workflow_store import WorkflowSnapshot


class SubtaskScheduler:
    """Compatibility-ready task scheduler.

    With a SqliteStore, ``run`` / ``resume`` perform durable ordered execution
    of bound, verifiable read-only tools or explicitly granted effectful
    adapters. The external controller must independently enforce fencing.
    """

    def __init__(self, registry: PluggableToolRegistry,
                 store: SqliteStore | None = None,
                 evidence_policies: Mapping[str, EvidencePolicy] | None = None,
                 execution_bridge: AuthorizedExecutionBridge | None = None) -> None:
        self.registry = registry
        self.validator = PlanValidator(registry)
        self.engine = (WorkflowOrchestrator(store, registry, evidence_policies,
                                            execution_bridge=execution_bridge)
                       if store is not None else None)

    def ready(self, plan: TaskPlan, completed: set[str]) -> tuple[Subtask, ...]:
        if not self.validator.is_validated(plan):
            raise DomainError("UNVALIDATED_TASK_PLAN")
        known = {s.subtask_id for s in plan.subtasks}
        if not completed.issubset(known):
            raise DomainError("UNKNOWN_COMPLETED_SUBTASK")
        return plan.ready(completed)

    def run(self, plan: TaskPlan, *, run_id: str | None = None) -> WorkflowSnapshot:
        if self.engine is None:
            raise DomainError("WORKFLOW_STORE_REQUIRED")
        return self.engine.run(plan, run_id=run_id)

    def resume(self, run_id: str) -> WorkflowSnapshot:
        if self.engine is None:
            raise DomainError("WORKFLOW_STORE_REQUIRED")
        return self.engine.resume(run_id)
