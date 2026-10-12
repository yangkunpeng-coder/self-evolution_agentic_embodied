"""Immutable, validated high-level plan format (not an authorization ticket)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Subtask:
    subtask_id: str
    tool_id: str
    arguments_json: str
    depends_on: tuple[str, ...]
    success_condition: str

    def arguments(self) -> dict[str, Any]:
        import json

        return json.loads(self.arguments_json)


@dataclass(frozen=True)
class TaskPlan:
    goal: str
    subtasks: tuple[Subtask, ...]
    planner_id: str = "openai-compatible-planner"
    # No implicit authority, and no claim of performance improvement.

    def ready(self, completed: set[str]) -> tuple[Subtask, ...]:
        return tuple(task for task in self.subtasks
                     if task.subtask_id not in completed
                     and all(dep in completed for dep in task.depends_on))
