"""Independent whitelist predicate verifier. Provider status is not acceptance."""
from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from ..core.canonical_json import content_hash
from ..core.contracts import EvidenceBundle, Postcondition, Verdict, VerificationVerdict
from ..core.ids import uid


def evidence_digest(e: EvidenceBundle) -> str:
    return content_hash(asdict(e))


class IndependentVerifier:
    ID = "mvp-independent-verifier-v1"

    def verify(self, e: EvidenceBundle, condition: Postcondition) -> VerificationVerdict:
        result = Verdict.INCONCLUSIVE
        obs = e.after
        if (obs is not None and e.before is not None and obs.task_id == e.task_id
            and obs.release_id == e.release_id and obs.invocation_id == e.invocation_id
            and e.before.task_id == e.task_id and e.before.release_id == e.release_id
            and e.before.invocation_id is None):
            try:
                age_ms = (datetime.now(UTC) - datetime.fromisoformat(obs.observed_at)).total_seconds() * 1000
                if 0 <= age_ms <= obs.ttl_ms:
                    current = obs.state
                    for key in condition.field.split("."):
                        if not isinstance(current, dict) or key not in current:
                            break
                        current = current[key]
                    else:
                        result = (Verdict.SUCCESS if condition.kind == "exists" or
                                  (type(current) is type(condition.expected) and current == condition.expected)
                                  else Verdict.FAILURE)
            except ValueError:
                pass
        return VerificationVerdict(uid(), e.task_id, e.step_id, e.scope,
                                   e.release_id, evidence_digest(e), condition.condition_id,
                                   result, self.ID)
