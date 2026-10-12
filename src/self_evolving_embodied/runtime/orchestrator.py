"""One-step Fake-only MVP: every external call is preceded by durable intent."""
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from threading import Event, Thread
from time import monotonic
from typing import cast

from ..adapters.fake.gateway import FakeGateway
from ..core.canonical_json import canonical_bytes
from ..core.contracts import (
    AuthorizedInvocation,
    DomainError,
    ProviderResponse,
    Status,
    TaskContract,
    TaskOutcome,
    Verdict,
)
from ..harness.authorizer import CapabilityAuthorizer
from ..harness.commit_gate import CommitGate
from ..harness.evidence import collect_evidence
from ..harness.recovery import RecoveryManager
from ..harness.skill_registry import SqliteSkillRegistry
from ..harness.verifier import IndependentVerifier
from ..runtime.planning import FakePlanner
from ..storage.sqlite.database import SqliteStore
from .context import compile_context


class _UncertainInvocation(Exception):
    """Dispatched invocation may still be acting; never retry."""
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _bounded_call(call: Callable[[], str], timeout_s: float) -> tuple[bool, str | Exception | None]:
    """Bound the *wait*, not the underlying thread or physical side effect.

    Thread remains daemonized if it hangs. Used ONLY by Fake adapter in R02.
    Real SDK integrations require provider-specific cancellation and isolation.
    """
    done = Event()
    box: dict[str, object] = {}

    def worker() -> None:
        try:
            box["value"] = call()
        except Exception as exc:  # noqa: BLE001
            box["error"] = exc
        finally:
            done.set()

    Thread(target=worker, name="r02-fake-adapter", daemon=True).start()
    if not done.wait(timeout_s):
        return False, None
    if "error" in box:
        return True, cast(Exception, box["error"])
    return True, cast(str, box["value"])


@dataclass
class FakeHarness:
    store: SqliteStore
    gateway: FakeGateway
    planner: FakePlanner
    stop_timeout_ms: int = 250

    def _invoke_bounded(self, ticket: AuthorizedInvocation, arguments: dict,
                        task_id: str, started: float, wall_ms: int) -> ProviderResponse:
        """Poll cancellation and deadline while the Fake invocation runs."""
        done = Event()
        box: dict[str, object] = {}

        def worker() -> None:
            try:
                box["value"] = self.gateway.invoke(ticket, arguments)
            except Exception as exc:  # noqa: BLE001
                # A provider may act before raising, so the failure is ambiguous.
                box["error"] = exc
            finally:
                done.set()

        Thread(target=worker, name="r02-fake-invoke", daemon=True).start()
        while True:
            self.store.heartbeat_worker()
            # Cancellation and budget checks intentionally win over a concurrent
            # late provider reply; late completion must not commit SUCCESS.
            if self.store.is_cancel_requested(task_id):
                raise _UncertainInvocation("CANCELLED")
            left = wall_ms / 1000 - (monotonic() - started)
            if left <= 0:
                raise _UncertainInvocation("TIMEOUT")
            if done.wait(min(0.02, left)):
                if self.store.is_cancel_requested(task_id):
                    raise _UncertainInvocation("CANCELLED")
                if (monotonic() - started) * 1000 >= wall_ms:
                    raise _UncertainInvocation("TIMEOUT")
                if "error" in box:
                    raise _UncertainInvocation("ADAPTER_EXCEPTION")
                return cast(ProviderResponse, box["value"])

    def _stop_and_quarantine(self, task_id: str, ticket: AuthorizedInvocation,
                             version: int, reason: str) -> TaskOutcome:
        # No device call may happen before the stop intent is durable.
        self.store.request_stop(task_id, ticket.invocation_id, version, reason)
        timeout_s = max(0.001, self.stop_timeout_ms / 1000)
        completed, outcome = _bounded_call(
            lambda: self.gateway.request_stop(ticket.invocation_id), timeout_s,
        )
        # A delayed ACK is only diagnostic and cannot mutate a terminal task.
        ack = outcome if completed and isinstance(outcome, str) else "UNKNOWN"
        if ack not in {"CONFIRMED", "REJECTED", "UNKNOWN"}:
            ack = "UNKNOWN"
        self.store.mark_unknown(task_id, ticket.invocation_id,
                                version, ticket.resource, ack)
        return self.store.outcome(task_id, "EXECUTION_UNKNOWN")

    def run(self, task: TaskContract) -> TaskOutcome:
        """Claim exclusive lease BEFORE any startup reconciliation or task work."""
        with self.store.worker_session():
            return self._run_owned(task)

    def _run_owned(self, task: TaskContract) -> TaskOutcome:
        """No real robot execution, no retry of uncertain side effects."""
        # Reconcile only when this single-worker Runtime starts a new task.
        # This never replays an external action, even for a recorded-only intent.
        self.store.recover_inflight()
        self.store.create_task(task)
        started = monotonic()
        try:
            if task.max_steps != 1:
                raise DomainError("MVP_ONE_STEP_ONLY")
            state = self.store.current(task.task_id)
            version = self.store.transition(task.task_id, state["state_version"],
                                            Status.RUNNING, "TASK_STARTED")
            pre = self.gateway.observe(task.task_id, task.release_id, None)
            if pre is None:
                raise DomainError("NO_PRE_OBSERVATION")
            context = compile_context(task, version, pre)
            plan = self.planner.plan(task, version, context)
            # Snapshot untrusted planner data before authorization. The planner
            # may retain and mutate its own dictionary; the execution path must
            # never dispatch values different from the authorized hash.
            arguments_snapshot = json.loads(canonical_bytes(plan.arguments))
            if not isinstance(arguments_snapshot, dict):
                raise DomainError("INVALID_ARGUMENTS")
            plan = replace(plan, arguments=arguments_snapshot)
            ticket = CapabilityAuthorizer(SqliteSkillRegistry(self.store)).authorize(task, plan, version)
            ticket = replace(ticket, fencing_token=self.store.active_fencing_token())
            self._check_deadline(started, task.wall_deadline_ms)
            if self.store.is_cancel_requested(task.task_id):
                raise DomainError("CANCELLED_BEFORE_DISPATCH")
            version = self.store.record_intent(ticket, plan)  # transaction FIRST
            version = self.store.mark_dispatched(task.task_id, ticket.invocation_id, version)
            # This protects DB-side dispatch orchestration; real devices MUST
            # verify the token themselves before accepting a physical command.
            self.store.active_fencing_token()
            try:
                response = self._invoke_bounded(ticket, plan.arguments, task.task_id,
                                                started, task.wall_deadline_ms)
            except _UncertainInvocation as uncertain:
                return self._stop_and_quarantine(task.task_id, ticket, version, uncertain.reason)
            version = self.store.record_response(task.task_id, response, version)
            try:
                post = self.gateway.observe(task.task_id, task.release_id, ticket.invocation_id)
            except Exception:  # noqa: BLE001 - external observation failure: fail closed
                self.store.quarantine_unverifiable_execution(
                    task.task_id, ticket.invocation_id, version, "POST_OBSERVATION_ERROR",
                )
                return self.store.outcome(task.task_id, "EXECUTION_UNKNOWN")
            step_evidence = collect_evidence(task.task_id, plan.step_id, ticket.invocation_id,
                                             task.release_id, pre, post, "STEP")
            verifier = IndependentVerifier()
            step_verdict = verifier.verify(step_evidence, plan.postcondition)
            self.store.save_evidence(step_evidence, step_verdict)
            if step_verdict.result != Verdict.SUCCESS:
                raise DomainError("STEP_" + step_verdict.result.value)
            self._check_deadline(started, task.wall_deadline_ms)
            CommitGate(self.store).commit(step_evidence, step_verdict, version,
                                          key=f"{task.task_id}:STEP:{plan.step_id}")
            version = self.store.current(task.task_id)["state_version"]
            # STEP success does not imply GOAL success: independent evidence/scope.
            version = self.store.transition(task.task_id, version, Status.VERIFYING, "GOAL_CHECK_STARTED")
            goal_evidence = collect_evidence(task.task_id, None, ticket.invocation_id,
                                             task.release_id, pre, post, "GOAL")
            goal_verdict = verifier.verify(goal_evidence, task.goal)
            self.store.save_evidence(goal_evidence, goal_verdict)
            if goal_verdict.result != Verdict.SUCCESS:
                raise DomainError("GOAL_" + goal_verdict.result.value)
            self._check_deadline(started, task.wall_deadline_ms)
            CommitGate(self.store).commit(goal_evidence, goal_verdict, version,
                                          key=f"{task.task_id}:GOAL")
            return self.store.outcome(task.task_id)
        except DomainError as exc:
            if exc.code in {"STALE_WORKER_FENCE", "WORKER_FENCE_REQUIRED"}:
                raise  # A superseded worker must never mutate task state.
            RecoveryManager.decision(failure_code=exc.code, retries_used=0,
                                     max_retries=task.max_retries)
            state = self.store.current(task.task_id)
            if state["status"] not in {Status.SUCCEEDED.value, Status.FAILED.value,
                                       Status.HUMAN_REVIEW.value}:
                self.store.fail_task(task.task_id, state["state_version"], exc.code)
            return self.store.outcome(task.task_id, exc.code)

    @staticmethod
    def _check_deadline(started: float, max_ms: int) -> None:
        if (monotonic() - started) * 1000 >= max_ms:
            raise DomainError("DEADLINE_EXCEEDED")
