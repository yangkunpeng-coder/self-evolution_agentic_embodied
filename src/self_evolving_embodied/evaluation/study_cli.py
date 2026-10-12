"""Read-only inspection entrypoint for offline research studies and release pointer.

Opening SqliteStore may upgrade the DB schema; back up before first use.
This command cannot evaluate, approve, publish or operate a robot.
"""
from __future__ import annotations

import argparse
import json
import sys

from ..core.contracts import DomainError
from ..governance.agent_policy_release import ResearchPolicyGovernance
from ..storage.sqlite.database import SqliteStore
from .agent_policy_study import IndependentPolicyEvaluator


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect independent offline policy research (never executes actions).")
    parser.add_argument("--db", required=True, help="Research SQLite file; back up before upgrade to schema v16")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--study-id", help="Show paired evaluation metrics and gate")
    group.add_argument("--target", choices=("OBSERVATION_ACQUISITION", "ASSESSMENT_ROUTING",
                                            "PLANNING_STRATEGY", "TOOL_SELECTION"),
                       help="Show approved offline research policy version")
    args = parser.parse_args(argv)
    try:
        store = SqliteStore(args.db)
        if args.study_id:
            report = IndependentPolicyEvaluator(store).get_study(args.study_id).report
            output = {key: report[key] for key in (
                "study_kind", "research_only", "proposal_id", "target", "suite_id",
                "case_count", "baseline_metrics", "candidate_metrics", "gate",
                "blockers", "production_release_authorized")}
        else:
            p = ResearchPolicyGovernance(store).pointer(args.target)
            output = {"scope": p.scope, "target": p.target,
                      "revision": p.revision, "active_policy_hash": p.active_policy_hash,
                      "active_study_id": p.active_study_id, "event_head_hash": p.event_head_hash,
                      "policy": p.policy.to_dict() if p.policy else None,
                      "live_runtime_changed": False}
    except DomainError as exc:
        print(f"Offline research inspection refused: {exc.code}", file=sys.stderr)
        return 2
    print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
