"""Planner proposes a step but cannot invoke any execution adapter."""
from ..core.contracts import PlanStep, Postcondition, TaskContract


class FakePlanner:
    def __init__(self, tool_id: str = "fake.move", capability_id: str = "fake.move",
                 arguments: dict | None = None) -> None:
        self.tool_id = tool_id
        self.capability_id = capability_id
        self.arguments = arguments if arguments is not None else {"target": "ready"}

    def plan(self, task: TaskContract, state_version: int, context: object | None = None) -> PlanStep:
        return PlanStep(
            step_id="step-1", tool_id=self.tool_id, capability_id=self.capability_id,
            arguments=self.arguments, postcondition=Postcondition("step-condition", "position", "ready"),
            release_id=task.release_id, expected_state_version=state_version,
            based_on_snapshot_id=getattr(context, "snapshot_id", ""),
        )
