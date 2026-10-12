"""ROS 2 sensor Topic -> existing in-memory Observation Pipeline.

ROS 2 performs Topic/DDS communication. This adapter only converts incoming
messages and pushes them immediately; it never polls and never writes a DB.
The app must spin its rclpy executor; no rclpy import occurs on Windows.
"""
from __future__ import annotations

import importlib
import math
import re
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType
from typing import Any, Callable

from ...observations import (
    ObservationInput, ObservationKey, ObservationSink, QueueObservationPipeline,
    SensorStreamConfig,
)

_ROS_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*/msg/[A-Za-z][A-Za-z0-9_]*$")

# Registering an arbitrary ROS message type is an application-side decision,
# never inferred from a model-generated task description.
TopicDecoder = Callable[[Any], tuple[object, dict[str, str], int | None]]


@dataclass(frozen=True)
class ROS2TopicBinding:
    topic: str
    message_type: str  # e.g. sensor_msgs/msg/Image
    key: ObservationKey
    frequency_hz: float
    time_domain: str | None = None
    retention_seconds: float | None = None
    qos_depth: int = 10
    decoder: TopicDecoder | None = None

    def __post_init__(self) -> None:
        if (not isinstance(self.topic, str) or not self.topic.startswith("/")
            or len(self.topic) > 256 or " " in self.topic):
            raise ValueError("ROS topic must be an absolute topic path")
        if not isinstance(self.message_type, str) or not _ROS_TYPE.fullmatch(self.message_type):
            raise ValueError("message_type must be 'pkg/msg/Message'")
        if not isinstance(self.key, ObservationKey):
            raise TypeError("key must be an ObservationKey")
        if (isinstance(self.frequency_hz, bool) or not isinstance(self.frequency_hz, (int, float))
            or not math.isfinite(self.frequency_hz) or self.frequency_hz <= 0):
            raise ValueError("frequency_hz must be a positive finite number")
        if (type(self.qos_depth) is not int or not 1 <= self.qos_depth <= 1000):
            raise ValueError("qos_depth must be in [1, 1000]")
        if self.time_domain is not None and (not isinstance(self.time_domain, str)
                                            or not self.time_domain.strip()):
            raise ValueError("time_domain must be None or a nonempty clock-domain name")
        if self.retention_seconds is not None and (
            isinstance(self.retention_seconds, bool) or
            not isinstance(self.retention_seconds, (float, int)) or
            not math.isfinite(self.retention_seconds) or self.retention_seconds <= 0
        ):
            raise ValueError("retention_seconds must be positive")
        if self.decoder is not None and not callable(self.decoder):
            raise TypeError("decoder must be callable")


@dataclass(frozen=True)
class ROS2IngressStats:
    received: int
    rejected: int
    last_error: str | None


class ROS2TopicIngress:
    """Explicit ROS 2 subscription ownership, separate from Queue ownership.

    A single instance may ingest via `accept(topic, msg)` even without rclpy,
    but `start(node)` creates actual ROS 2 subscriptions. The node/executor and
    underlying robot remain managed by the caller. No automatic reconnect.
    """

    def __init__(self, sink: ObservationSink,
                 bindings: tuple[ROS2TopicBinding, ...]) -> None:
        if not callable(getattr(sink, "push", None)):
            raise TypeError("sink must implement ObservationSink.push")
        if not isinstance(bindings, tuple) or not bindings:
            raise ValueError("at least one binding is required")
        if any(not isinstance(b, ROS2TopicBinding) for b in bindings):
            raise TypeError("bindings must be ROS2TopicBinding instances")
        if len({b.topic for b in bindings}) != len(bindings) or len({b.key for b in bindings}) != len(bindings):
            raise ValueError("duplicate ROS topic or observation stream")
        self._sink = sink
        self._bindings = {b.topic: b for b in bindings}
        self._lock = RLock()
        self._node: Any = None
        self._subscriptions: list[Any] = []
        self._closed = False
        self._received = 0
        self._rejected = 0
        self._last_error: str | None = None
        # Register all source-specific queues before the first ROS callback.
        # Existing streams are not silently reconfigured.
        # Standalone Pipeline ownership retains automatic registration. With a
        # framework-owned sink, the host declares streams on FrameworkBuilder.
        if isinstance(sink, QueueObservationPipeline):
            for binding in bindings:
                sink.register_stream(SensorStreamConfig(
                    binding.key, binding.frequency_hz, binding.retention_seconds
                ))

    def start(self, node: Any) -> None:
        """Attach ROS Topic subscribers; caller MUST spin `node` separately.

        SensorDataQoS is chosen for common sensor messages, with configurable
        depth. Consumers with incompatible QoS must configure ROS upstream.
        """
        with self._lock:
            if self._closed or self._node is not None:
                raise RuntimeError("ROS ingress closed or already started")
            if not callable(getattr(node, "create_subscription", None)):
                raise TypeError("node must support create_subscription")
            try:
                qos_mod = importlib.import_module("rclpy.qos")
                qos_profile_type = qos_mod.QoSProfile
                policy = qos_mod.ReliabilityPolicy.BEST_EFFORT
                durability = qos_mod.DurabilityPolicy.VOLATILE
            except ImportError as exc:
                raise RuntimeError("ROS 2 rclpy is required only on the ROS host") from exc
            subscriptions: list[Any] = []
            try:
                for binding in self._bindings.values():
                    package, _, name = binding.message_type.split("/")
                    cls = getattr(importlib.import_module(f"{package}.msg"), name)
                    qos = qos_profile_type(depth=binding.qos_depth,
                                           reliability=policy, durability=durability)
                    callback = lambda msg, topic=binding.topic: self.accept(topic, msg)
                    subscriptions.append(node.create_subscription(cls, binding.topic,
                                                                  callback, qos))
            except Exception:
                for subscription in subscriptions:
                    try:
                        node.destroy_subscription(subscription)
                    except Exception:
                        pass
                raise
            self._node = node
            self._subscriptions = subscriptions

    def accept(self, topic: str, message: Any) -> None:
        """Called by ROS callbacks: decode and immediately push (no polling)."""
        binding = self._bindings.get(topic)
        if binding is None:
            raise KeyError(f"unknown ROS topic: {topic}")
        with self._lock:
            if self._closed:
                return
        try:
            decoder = binding.decoder or _built_in_decoder(binding.message_type)
            payload, metadata, explicit_size = decoder(message)
            if isinstance(payload, dict):
                payload = MappingProxyType(payload)
            timestamp_ns, frame_id = _extract_header(message)
            self._sink.push(ObservationInput(
                key=binding.key, payload=payload,
                source_timestamp_ns=timestamp_ns, time_domain=(binding.time_domain if timestamp_ns is not None else None),
                frame_id=frame_id, metadata={"ros_topic": topic, "ros_type": binding.message_type, **metadata},
                payload_size_bytes=explicit_size,
            ))
        except Exception as exc:
            # Callback exceptions never turn missing observations into success.
            # Keep error telemetry bounded; do not print on every high-rate frame.
            with self._lock:
                self._rejected += 1
                self._last_error = f"{type(exc).__name__}: {str(exc)[:160]}"
            return
        with self._lock:
            self._received += 1

    def stats(self) -> ROS2IngressStats:
        with self._lock:
            return ROS2IngressStats(self._received, self._rejected, self._last_error)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            node, subscriptions = self._node, self._subscriptions
            self._node, self._subscriptions = None, []
        if node is not None:
            first_error = None
            for subscription in subscriptions:
                try:
                    node.destroy_subscription(subscription)
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
            if first_error is not None:
                raise first_error
        # Do NOT close the Observation Pipeline: other adapters may own streams.


def _extract_header(message: Any) -> tuple[int | None, str | None]:
    header = getattr(message, "header", None)
    stamp = getattr(header, "stamp", None)
    frame = getattr(header, "frame_id", None)
    if stamp is None:
        return None, frame if isinstance(frame, str) else None
    sec = getattr(stamp, "sec", None)
    nanosec = getattr(stamp, "nanosec", None)
    if type(sec) is not int or type(nanosec) is not int or sec < 0 or not 0 <= nanosec < 1_000_000_000:
        return None, frame if isinstance(frame, str) else None
    return sec * 1_000_000_000 + nanosec, frame if isinstance(frame, str) else None


def _built_in_decoder(msg_type: str) -> TopicDecoder:
    if msg_type in {"sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage", "sensor_msgs/msg/PointCloud2"}:
        def decode_binary(msg: Any) -> tuple[object, dict[str, str], int | None]:
            # Copy data to immutable bytes. ROS message memory may be recycled
            # once the subscription callback returns; tools may retain records.
            raw = bytes(msg.data)
            properties: dict[str, str] = {}
            for attr in ("width", "height", "encoding", "step", "format",
                         "point_step", "row_step", "is_bigendian", "is_dense"):
                value = getattr(msg, attr, None)
                if value is not None:
                    properties[attr] = str(value)
            if msg_type.endswith("PointCloud2"):
                properties["point_fields"] = ",".join(
                    f"{f.name}:{f.offset}:{f.datatype}:{f.count}"
                    for f in getattr(msg, "fields", ())
                )
            return raw, properties, len(raw)
        return decode_binary
    if msg_type == "sensor_msgs/msg/JointState":
        def decode_joint(msg: Any) -> tuple[object, dict[str, str], int | None]:
            return {
                "name": tuple(msg.name),
                "position": tuple(float(x) for x in msg.position),
                "velocity": tuple(float(x) for x in msg.velocity),
                "effort": tuple(float(x) for x in msg.effort),
            }, {}, (sum(len(str(n).encode("utf-8")) for n in msg.name)
                      + 8 * (len(msg.position) + len(msg.velocity) + len(msg.effort)))
        return decode_joint
    if msg_type == "sensor_msgs/msg/Imu":
        def decode_imu(msg: Any) -> tuple[object, dict[str, str], int | None]:
            def xyz(q: Any) -> tuple[float, ...]:
                return (float(q.x), float(q.y), float(q.z))
            o = msg.orientation
            return {
                "orientation_xyzw": (float(o.x), float(o.y), float(o.z), float(o.w)),
                "angular_velocity": xyz(msg.angular_velocity),
                "linear_acceleration": xyz(msg.linear_acceleration),
            }, {}, 10 * 8
        return decode_imu
    if msg_type == "sensor_msgs/msg/LaserScan":
        def decode_scan(msg: Any) -> tuple[object, dict[str, str], int | None]:
            from array import array
            ranges = array("f", msg.ranges).tobytes()
            intensities = array("f", msg.intensities).tobytes()
            return {
                "ranges_f32": ranges,
                "intensities_f32": intensities,
            }, {
                "dtype": "float32_native_endian",
                "range_count": str(len(msg.ranges)),
                "intensity_count": str(len(msg.intensities)),
                "angle_min": str(msg.angle_min),
                "angle_increment": str(msg.angle_increment),
                "range_min": str(msg.range_min),
                "range_max": str(msg.range_max),
            }, len(ranges) + len(intensities)
        return decode_scan
    raise ValueError(f"No built-in converter for {msg_type}; supply a trusted decoder")
