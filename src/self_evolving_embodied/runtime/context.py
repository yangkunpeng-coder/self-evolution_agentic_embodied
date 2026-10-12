"""Read-only Context Snapshot for the one-step Fake Planner."""

from dataclasses import asdict, dataclass

from ..core.canonical_json import content_hash
from ..core.contracts import Observation, TaskContract
from ..core.ids import uid, utc_now


@dataclass(frozen=True)
class ContextSnapshot:
    snapshot_id: str
    task_id: str
    release_id: str
    state_version: int
    observation_hash: str
    compiled_at: str
    visible_tool_ids: tuple[str, ...]


def compile_context(task: TaskContract, state_version: int,
                    observation: Observation) -> ContextSnapshot:
    if observation.task_id != task.task_id or observation.release_id != task.release_id:
        raise ValueError("observation belongs to another task/release")
    return ContextSnapshot(uid(), task.task_id, task.release_id, state_version,
                           content_hash(asdict(observation)), utc_now(), ("fake.move",))
