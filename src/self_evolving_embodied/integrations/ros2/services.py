"""Optional native ROS 2 Service -> READ_ONLY Execution Toolbox handler.

A ROS Service is not automatically read-only. Trusted application code must
approve a *specific* service endpoint before this handler may be bound to a
READ_ONLY ToolSpec. ROS executor must spin independently of this caller.
"""
from __future__ import annotations

import math
from threading import Event
from typing import Any, Callable, Mapping

from ...core.contracts import DomainError


def _await_ros_future(future: Any, timeout_seconds: float) -> Any:
    """Wait for an async rclpy future without calling spin_until_future_complete.

    Calling thread must NOT be the sole ROS executor callback; the user must
    spin the node on an independent executor thread, else this times out.
    """
    done = Event()
    future.add_done_callback(lambda _: done.set())
    if not done.wait(timeout_seconds):
        try:
            future.cancel()
        except Exception:
            pass
        raise TimeoutError("ROS2_REQUEST_TIMEOUT")
    return future.result()


class ROS2ReadOnlyServiceHandler:
    """Trusted ROS 2 service Handler; conforms to ToolHandler.invoke.

    `client` is returned from rclpy Node.create_client, but rclpy is never
    imported here. Request/response codecs isolate ROS wire schemas from
    framework tool schemas. The existing registry checks arguments/output.
    """

    def __init__(self, client: Any,
                 request_builder: Callable[[Mapping[str, Any]], Any],
                 response_reader: Callable[[Any], Mapping[str, Any]], *,
                 trusted_read_only: bool = False,
                 timeout_seconds: float = 5.0) -> None:
        if trusted_read_only is not True:
            raise DomainError("ROS_SERVICE_READ_ONLY_APPROVAL_REQUIRED")
        if not callable(getattr(client, "call_async", None)) or not callable(getattr(client, "service_is_ready", None)):
            raise TypeError("client must be a ROS 2 service client")
        if not callable(request_builder) or not callable(response_reader):
            raise TypeError("service codecs must be callable")
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds) or not 0.1 <= timeout_seconds <= 120.0):
            raise ValueError("invalid service timeout")
        self._client = client
        self._build = request_builder
        self._read = response_reader
        self._timeout = float(timeout_seconds)

    def invoke(self, arguments: dict[str, Any]) -> Mapping[str, Any]:
        if not self._client.service_is_ready():
            raise DomainError("ROS_SERVICE_UNAVAILABLE")
        try:
            request = self._build(arguments)
            reply = _await_ros_future(self._client.call_async(request), self._timeout)
            result = self._read(reply)
        except TimeoutError:
            raise DomainError("ROS_SERVICE_TIMEOUT") from None
        except DomainError:
            raise
        except Exception:
            raise DomainError("ROS_SERVICE_CALL_FAILED") from None
        if not isinstance(result, Mapping):
            raise DomainError("ROS_SERVICE_RESULT_INVALID")
        return dict(result)
