"""ROS 2 wiring template: run this only on a host with a compatible ROS 2.

This is a configuration reference, not a test or an executable robot demo.
No motors or ROS actions are sent by importing/running this file.

The application owns rclpy.init(), its Node, a separately spinning Executor,
controller-side authorization endpoints and action/observation codecs.
"""
from self_evolving_embodied.observations import (
    ObservationKey, ObservationRequest, QueueObservationPipeline,
)
from self_evolving_embodied.integrations.ros2 import (
    ROS2TopicBinding, ROS2TopicIngress,
)
from self_evolving_embodied.toolboxes.common import ObservationNeeds


def build_observation_ingress(node):
    """Subscribe to an existing robot's ROS Topic feeds; does not spin node."""
    pipeline = QueueObservationPipeline()  # default retention = 60 seconds
    ingress = ROS2TopicIngress(pipeline, (
        ROS2TopicBinding(
            topic="/camera/color/image_raw",
            message_type="sensor_msgs/msg/Image",
            key=ObservationKey("front_camera", "rgb"),
            frequency_hz=30.0,
            time_domain="ros2:robot_clock",
        ),
        ROS2TopicBinding(
            topic="/joint_states",
            message_type="sensor_msgs/msg/JointState",
            key=ObservationKey("arm", "joint_state"),
            frequency_hz=100.0,
            time_domain="ros2:robot_clock",
        ),
    ))
    ingress.start(node)  # ROS 2 imports happen here, not at package import.
    return pipeline, ingress


VLA_OBSERVATIONS = ObservationNeeds(required=(
    ObservationRequest(ObservationKey("front_camera", "rgb"), max_age_ms=250),
    ObservationRequest(ObservationKey("arm", "joint_state"), max_age_ms=250),
))


# Physical commands must be registered and bound SEPARATELY:
# 1. Explicit PHYSICAL_ACTION ToolSpec in the existing PluggableToolRegistry.
# 2. A trusted controller-side fence enforcing ticket/resource/epoch remotely.
# 3. ROS2FencedActionAdapter with a typed ROS ActionClient and codecs; its
#    goal_builder must transmit the ticket to a server that enforces the fence.
# 4. ExecutionGrant via AuthorizedExecutionBridge.bind(...).
# 5. WorkflowOrchestrator with trusted evidence_policies and assessment routes.
# A bare Nav2 or MoveIt ROS ActionClient MUST NOT be treated as a fenced endpoint.
