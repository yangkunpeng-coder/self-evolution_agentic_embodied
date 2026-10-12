"""Offline, non-executing Single-Agent workflow evolution projection command."""
from __future__ import annotations

import argparse
import json
import sys

from ..core.contracts import DomainError
from ..storage.sqlite.database import SqliteStore
from .workflow_cycle import WorkflowEvolutionCycle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Project terminal Single-Agent workflow evidence into offline "
                    "experience, diagnosis and non-executable review proposals. "
                    "Never calls a model or robot and never changes live policies."
    )
    parser.add_argument("--db", required=True, help="SQLite path; back up before v15 upgrade")
    parser.add_argument("--limit", type=int, default=100,
                        help="Maximum items processed per stage, 1..1000 (default: 100)")
    args = parser.parse_args(argv)
    try:
        store = SqliteStore(args.db)
        with store.worker_session():
            counts = WorkflowEvolutionCycle(store).run_cycle(args.limit)
    except DomainError as exc:
        print(f"Offline workflow evolution refused: {exc.code}", file=sys.stderr)
        return 2
    print(json.dumps({"mode": "OFFLINE_NON_EXECUTABLE", "schema_version": 15,
                      "workflow_policy_changed": False, "robot_actions_executed": False,
                      **counts}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
