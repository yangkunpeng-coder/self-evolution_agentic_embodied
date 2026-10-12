"""Fail-closed validation of untrusted LLM JSON plans against a trusted catalog."""
from __future__ import annotations

import json
import re
from typing import Any

from ..core.contracts import DomainError
from ..tools.contracts import check_arguments
from ..tools.registry import PluggableToolRegistry
from .contracts import Subtask, TaskPlan

_STEP_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,31}$")
_MAX_TASKS = 20


class PlanValidator:
    def __init__(self, registry: PluggableToolRegistry) -> None:
        self.registry = registry

    def parse(self, response_text: str, requested_goal: str) -> TaskPlan:
        if not isinstance(response_text, str):
            raise DomainError("INVALID_PLAN_JSON")
        try:
            byte_length = len(response_text.encode("utf-8"))
        except UnicodeError:
            raise DomainError("INVALID_PLAN_JSON") from None
        if byte_length > 32_768:
            raise DomainError("PLAN_TOO_LARGE")
        if not isinstance(requested_goal, str) or not 1 <= len(requested_goal.strip()) <= 4000:
            raise DomainError("INVALID_USER_GOAL")
        def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise DomainError("DUPLICATE_PLAN_KEY")
                result[key] = value
            return result

        def reject_constant(_: str) -> None:
            raise DomainError("INVALID_PLAN_JSON")

        try:
            obj = json.loads(response_text, object_pairs_hook=unique_pairs,
                             parse_constant=reject_constant)
        except (json.JSONDecodeError, ValueError, TypeError):
            raise DomainError("INVALID_PLAN_JSON") from None
        if not isinstance(obj, dict) or set(obj) != {"goal", "subtasks"}:
            raise DomainError("INVALID_PLAN_SCHEMA")
        if obj["goal"] != requested_goal:
            raise DomainError("PLAN_GOAL_MISMATCH")
        raw = obj["subtasks"]
        if not isinstance(raw, list) or not 1 <= len(raw) <= _MAX_TASKS:
            raise DomainError("INVALID_SUBTASK_COUNT")
        built: list[Subtask] = []
        known: set[str] = set()
        for item in raw:
            if (not isinstance(item, dict) or set(item) != {
                "id", "tool_id", "arguments", "depends_on", "success_condition"
            }):
                raise DomainError("INVALID_SUBTASK_SCHEMA")
            step_id = item["id"]
            if (not isinstance(step_id, str) or not _STEP_ID.fullmatch(step_id)
                or step_id in known):
                raise DomainError("INVALID_SUBTASK_ID")
            tool_id = item["tool_id"]
            spec = self.registry.resolve(tool_id)
            arguments = check_arguments(spec, item["arguments"])
            if (not isinstance(item["success_condition"], str)
                or item["success_condition"] not in spec.success_conditions):
                raise DomainError("UNKNOWN_SUCCESS_CONDITION")
            deps = item["depends_on"]
            if (not isinstance(deps, list) or len(deps) > _MAX_TASKS
                or any(not isinstance(x, str) for x in deps)
                or len(set(deps)) != len(deps)
                or any(dep not in known for dep in deps)):
                # Ordered topological plan. No forward refs, unknown refs or cycles.
                raise DomainError("INVALID_SUBTASK_DEPENDENCIES")
            known.add(step_id)
            built.append(Subtask(step_id, spec.tool_id,
                                 json.dumps(arguments, ensure_ascii=False, sort_keys=True,
                                            separators=(",", ":")),
                                 tuple(deps), item["success_condition"]))
        return TaskPlan(goal=requested_goal, subtasks=tuple(built))

    def is_validated(self, plan: object) -> bool:
        """Plans from another source must be revalidated against the *current* registry."""
        if not isinstance(plan, TaskPlan):
            return False
        try:
            obj: dict[str, Any] = {
                "goal": plan.goal,
                "subtasks": [{
                    "id": s.subtask_id, "tool_id": s.tool_id, "arguments": s.arguments(),
                    "depends_on": list(s.depends_on), "success_condition": s.success_condition,
                } for s in plan.subtasks],
            }
            self.parse(json.dumps(obj, ensure_ascii=False), plan.goal)
            return True
        except (DomainError, TypeError, ValueError):
            return False
