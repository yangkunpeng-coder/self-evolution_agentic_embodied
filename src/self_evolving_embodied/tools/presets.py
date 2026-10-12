"""Example capability declarations. No model or robot implementation is installed."""
from .contracts import Effect, ToolKind, ToolSpec
from .registry import PluggableToolRegistry


def research_tool_catalog() -> PluggableToolRegistry:
    """Create a purely declarative catalog for planning demonstrations."""
    registry = PluggableToolRegistry()
    registry.register(ToolSpec(
        "perception.vlm", "perception.describe", ToolKind.VLM,
        "Describe or locate a named scene object; requires a separate VLM implementation.",
        (("query", "string"),), ("perception_evidence_available",),
        effect=Effect.READ_ONLY,
    ))
    registry.register(ToolSpec(
        "navigation.vln", "navigation.navigate", ToolKind.VLN,
        "Navigate to a named destination; requires a navigation adapter.",
        (("destination", "string"),), ("destination_verified",),
        effect=Effect.PHYSICAL_ACTION,
    ))
    registry.register(ToolSpec(
        "manipulation.vla", "manipulation.execute", ToolKind.VLA,
        "Manipulate an object under a constrained embodied execution adapter.",
        (("instruction", "string"),), ("manipulation_verified",),
        effect=Effect.PHYSICAL_ACTION,
    ))
    registry.register(ToolSpec(
        "prediction.wam", "prediction.forecast", ToolKind.WAM,
        "Predict a possible state transition, not ground-truth evidence.",
        (("question", "string"),), ("prediction_available",),
        effect=Effect.READ_ONLY,
    ))
    registry.register(ToolSpec(
        "planning.classical", "planning.compute", ToolKind.CLASSICAL_PLANNER,
        "Compute a classical path or motion plan for a goal.",
        (("goal", "string"),), ("computed_plan_available",),
        effect=Effect.READ_ONLY,
    ))
    registry.register(ToolSpec(
        "control.classical", "control.execute", ToolKind.CONTROLLER,
        "Run a bounded classical controller via a future Harness integration.",
        (("command", "string"),), ("control_result_verified",),
        effect=Effect.PHYSICAL_ACTION,
    ))
    return registry
