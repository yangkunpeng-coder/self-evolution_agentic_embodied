"""ROS 2 Action adapter for the existing AuthorizedExecutionBridge.

No standalone action dispatch API is exposed. This adapter must be bound with
ExecutionGrant using AuthorizedExecutionBridge, which persists intent first.
A standard rclpy ActionClient by itself is NOT a device-side fencing system.
A trusted controller-side fence MUST independently reject stale epochs and
validate the ticket for this physical resource before goal acceptance.
"""
from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import RLock
from time import monotonic
from typing import Any, Protocol

from ...core.contracts import DomainError
from ...harness.execution_bridge import ExecutionTicket
from .services import _await_ros_future


class ControllerSideFence(Protocol):
    """MANDATORY trusted remote/controller-side epoch acceptance protocol.

    Implementation must enforce resource/epoch at the actuator-side command
    admission boundary; merely storing tokens in the Agent process is unsafe.
    This interface cannot prove that such enforcement is correctly deployed.
    """

    def enforce_at_controller(self, ticket: ExecutionTicket) -> bool: ...


@dataclass
class _ActiveGoal:
    handle: Any | None = None
    stop_requested: bool = False
    completed: bool = False


class ROS2FencedActionAdapter:
    """Bridge-compatible ROS 2 Action adapter with explicitly injected safety ports.

    The host constructs an rclpy.action.ActionClient for a particular action
    schema. Its goal_builder MUST embed the authorized ticket/resource/epoch
    in an action protocol verified by the remote endpoint; do NOT attach a
    bare Nav2/MoveIt action client without a real controller-side gate.

    The result_reader returns a compact *provider status*, NOT a semantic
    success verdict. independent_observe reads separate robot observations
    and returns small JSON-serializable state; raw images never enter SQLite.
    """

    def __init__(self, client: Any,
                 fence: ControllerSideFence,
                 goal_builder: Callable[[ExecutionTicket, Mapping[str, Any]], Any],
                 result_reader: Callable[[Any], Mapping[str, Any]],
                 independent_observe: Callable[[ExecutionTicket], Mapping[str, Any]], *,
                 goal_accept_timeout_seconds: float = 5.0) -> None:
        if not callable(getattr(client, "send_goal_async", None)):
            raise TypeError("client must be rclpy ActionClient or equivalent")
        if not callable(getattr(fence, "enforce_at_controller", None)):
            raise DomainError("CONTROLLER_SIDE_FENCING_REQUIRED")
        if not all(callable(fn) for fn in (goal_builder, result_reader, independent_observe)):
            raise TypeError("action codecs and observation provider must be callable")
        if (isinstance(goal_accept_timeout_seconds, bool)
            or not isinstance(goal_accept_timeout_seconds, (int, float))
            or not math.isfinite(goal_accept_timeout_seconds)
            or not 0.1 <= goal_accept_timeout_seconds <= 120):
            raise ValueError("invalid goal acceptance timeout")
        self._client = client
        self._fence = fence
        self._build = goal_builder
        self._read = result_reader
        self._observe = independent_observe
        self._accept_timeout = float(goal_accept_timeout_seconds)
        self._lock = RLock()
        self._active: dict[str, _ActiveGoal] = {}
        self._authorized: set[str] = set()

    def enforce_fencing(self, ticket: ExecutionTicket) -> bool:
        if not isinstance(ticket, ExecutionTicket):
            return False
        try:
            accepted = self._fence.enforce_at_controller(ticket) is True
        except Exception:
            return False
        if accepted:
            with self._lock:
                # Even a valid remote fence never authorizes replay of an
                # already dispatched invocation at this adapter.
                if (ticket.invocation_id in self._authorized
                    or ticket.invocation_id in self._active):
                    return False
                self._authorized.add(ticket.invocation_id)
        return accepted

    def observe(self, ticket: ExecutionTicket) -> Mapping[str, Any]:
        observation = self._observe(ticket)
        if not isinstance(observation, Mapping):
            raise DomainError("INDEPENDENT_ROBOT_OBSERVATION_MISSING")
        with self._lock:
            active = self._active.get(ticket.invocation_id)
            # Post-action independent observation closes a completed invocation.
            # Unknown/still-active goals remain available for stop diagnostics.
            if active is not None and active.completed:
                self._active.pop(ticket.invocation_id, None)
        return dict(observation)

    def invoke(self, ticket: ExecutionTicket,
               arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        with self._lock:
            if ticket.invocation_id not in self._authorized or ticket.invocation_id in self._active:
                raise DomainError("ROS_ACTION_NOT_FENCED_OR_ALREADY_SENT")
            state = _ActiveGoal()
            self._active[ticket.invocation_id] = state
            self._authorized.remove(ticket.invocation_id)
        deadline = monotonic() + ticket.timeout_seconds
        try:
            # The goal builder is trusted: remote action server must verify its
            # ticket and epoch, not just infer authority from the caller.
            goal = self._build(ticket, arguments)
            with self._lock:
                if state.stop_requested:
                    raise DomainError("ROS_ACTION_STOP_REQUESTED")
            send_future = self._client.send_goal_async(goal)

            def on_goal_ready(fut: Any) -> None:
                try:
                    handle = fut.result()
                    if handle is None or not handle.accepted:
                        return
                    with self._lock:
                        state.handle = handle
                        should_cancel = state.stop_requested
                    if should_cancel:
                        self._try_cancel(handle)
                except Exception:
                    pass

            # Guard against a late goal acceptance after caller timeout.
            send_future.add_done_callback(on_goal_ready)
            handle = _await_ros_future(send_future,
                                       min(self._accept_timeout, max(0.01, deadline - monotonic())))
            if handle is None or not handle.accepted:
                raise DomainError("ROS_ACTION_GOAL_REJECTED")
            with self._lock:
                state.handle = handle
                stop_requested = state.stop_requested
            if stop_requested:
                self._try_cancel(handle)
                raise DomainError("ROS_ACTION_STOP_REQUESTED")
            result_future = handle.get_result_async()
            reply = _await_ros_future(result_future, max(0.01, deadline - monotonic()))
            # ROS status is a protocol status, not evidence of target success.
            result = self._read(reply)
            if not isinstance(result, Mapping):
                raise DomainError("ROS_ACTION_RESULT_INVALID")
            with self._lock:
                if state.stop_requested:
                    raise DomainError("ROS_ACTION_STOP_REQUESTED")
                state.completed = True
            return dict(result)
        except TimeoutError:
            with self._lock:
                state.stop_requested = True
                handle = state.handle
            if handle is not None:
                self._try_cancel(handle)
            raise DomainError("ROS_ACTION_RESULT_UNKNOWN") from None
        finally:
            # Active goal handle deliberately retained for stop diagnostics;
            # it is not authority to re-dispatch an old invocation.
            pass

    def request_stop(self, ticket: ExecutionTicket) -> str:
        with self._lock:
            state = self._active.get(ticket.invocation_id)
            if state is None:
                return "UNKNOWN"
            state.stop_requested = True
            handle = state.handle
        if handle is not None:
            self._try_cancel(handle)
        # ROS cancel request/ack is not proof of physical stoppage.
        return "UNKNOWN"

    @staticmethod
    def _try_cancel(handle: Any) -> None:
        try:
            handle.cancel_goal_async()
        except Exception:
            pass
