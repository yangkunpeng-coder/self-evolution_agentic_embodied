"""Single invocation authorization from trusted registry and policy."""

from ..core.canonical_json import content_hash
from ..core.contracts import (
    AuthorizedInvocation,
    DomainError,
    PlanStep,
    TaskContract,
    ToolDescriptor,
)
from ..core.ids import uid


class ToolRegistry:
    def __init__(self) -> None:
        self._tools = {"fake.move": ToolDescriptor("fake.move", "fake.move", ("target",))}

    def get(self, tool_id: str) -> ToolDescriptor:
        tool = self._tools.get(tool_id)
        if tool is None:
            raise DomainError("UNKNOWN_TOOL")
        return tool


class CapabilityAuthorizer:
    def __init__(self, registry: object) -> None:
        self.registry = registry

    def authorize(self, task: TaskContract, plan: PlanStep, state_version: int) -> AuthorizedInvocation:
        # Registry is a trusted in-process component, never user-controlled.
        from .skill_registry import SqliteSkillRegistry

        if isinstance(self.registry, SqliteSkillRegistry):
            tool = self.registry.resolve(task, plan.tool_id)
        elif isinstance(self.registry, ToolRegistry):
            tool = self.registry.get(plan.tool_id)
        else:
            raise DomainError("UNSUPPORTED_TOOL_REGISTRY")
        if (plan.release_id != task.release_id or tool.release_id != task.release_id
            or plan.expected_state_version != state_version):
            raise DomainError("STALE_AUTHORIZATION")
        if (plan.capability_id != tool.capability_id
            or plan.capability_id not in task.allowed_capabilities):
            raise DomainError("CAPABILITY_DENIED")
        if not plan.postcondition or not task.goal:
            raise DomainError("UNVERIFIABLE")
        if set(plan.arguments) != set(tool.arguments_allowlist):
            raise DomainError("PARAMETERS_DENIED")
        if not isinstance(plan.arguments["target"], str) or len(plan.arguments["target"]) > 100:
            raise DomainError("INVALID_ARGUMENTS")
        digest = content_hash(plan.arguments)
        return AuthorizedInvocation(task.task_id, plan.step_id, uid(), task.release_id,
                                    tool.tool_id, state_version, digest, tool.resource)
