"""M10 collects evidence references; it cannot assign a verification result."""
from ..core.contracts import EvidenceBundle, Observation
from ..core.ids import uid


def collect_evidence(task_id: str, step_id: str | None, invocation_id: str,
                     release_id: str, before: Observation | None,
                     after: Observation | None, scope: str) -> EvidenceBundle:
    return EvidenceBundle(uid(), task_id, step_id, invocation_id, release_id,
                          before, after, scope)
