"""M12 trusted Commit Gate facade; storage enforces CAS and re-verifies persisted evidence."""
from ..core.contracts import EvidenceBundle, VerificationVerdict
from ..storage.sqlite.database import SqliteStore


class CommitGate:
    def __init__(self, store: SqliteStore) -> None:
        self._store = store

    def commit(self, evidence: EvidenceBundle, verdict: VerificationVerdict,
               expected_state_version: int, key: str) -> str:
        return self._store.commit(evidence.task_id, evidence, verdict, expected_state_version, key)
