"""CLI for deterministic Fake demonstration; no external model or robot is called."""

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from . import __version__
from .adapters.fake.gateway import FakeGateway
from .core.contracts import Postcondition, TaskContract
from .core.ids import uid
from .runtime.orchestrator import FakeHarness
from .runtime.planning import FakePlanner
from .storage.sqlite.database import SqliteStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Step 6 Fake-only evidence-gated agent demo")
    parser.add_argument("--scenario", choices=["success", "goal_fail", "unauthorized",
                                               "missing_after", "unknown_stop", "slow_invoke", "slow_stop",
                                               "rejected_stop"], default="success")
    parser.add_argument("--deadline-ms", type=int, default=30_000,
                        help="Fake task wall deadline; a slow invocation is never forcibly killed")
    parser.add_argument("--db", type=Path, default=Path("framework_runtime.sqlite3"))
    parser.add_argument("--recover-only", action="store_true",
                        help="Reconcile orphaned tasks without invoking any adapter")
    args = parser.parse_args(argv)
    store = SqliteStore(args.db)
    if args.recover_only:
        with store.worker_session():
            recovered = store.recover_inflight()
        print(json.dumps({"recovered": recovered, "count": len(recovered)},
                         ensure_ascii=False, indent=2))
        return 0
    gateway = FakeGateway(args.scenario)
    planner = FakePlanner(tool_id="unapproved.tool" if args.scenario == "unauthorized" else "fake.move")
    task = TaskContract(uid(), Postcondition("goal-condition", "goal_reached", True), ("fake.move",),
                        wall_deadline_ms=args.deadline_ms)
    outcome = FakeHarness(store, gateway, planner).run(task)
    print(json.dumps({**asdict(outcome), "events": store.event_kinds(task.task_id),
                      "fake_invocations": len(gateway.invocations), "version": __version__},
                     ensure_ascii=False, indent=2))
    return 0 if outcome.status.value == "SUCCEEDED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
