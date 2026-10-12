"""Trusted R0-only preconfigured release resolver; publishing is disabled in MVP."""
from ..core.contracts import DomainError
from ..storage.sqlite.database import SqliteStore


class ReleaseResolver:
    def __init__(self, store: SqliteStore) -> None:
        self._store = store

    def assert_available(self, requested_release_id: str) -> None:
        if requested_release_id != "R0-FAKE":
            raise DomainError("CANDIDATE_PUBLICATION_DISABLED")
        # Actual pin and manifest hash verification occur atomically at task creation.
