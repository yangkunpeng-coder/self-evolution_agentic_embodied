"""Online LLM planning command. Generates validated *proposals*, executes nothing."""
from __future__ import annotations

import argparse
import json
import sys

from ..adapters.llm import OpenAICompatibleConfig, OpenAICompatibleProvider
from ..core.contracts import DomainError
from ..runtime.workflow_orchestrator import WorkflowOrchestrator
from ..storage.sqlite.database import SqliteStore
from ..tools.presets import research_tool_catalog
from .llm_planner import LLMTaskPlanner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create a registry-validated task plan and optionally persist it (no physical action)."
    )
    parser.add_argument("--goal", required=True, help="Human natural-language instruction")
    parser.add_argument("--db", help="Persist an auditable workflow in SQLite schema v13")
    parser.add_argument("--run-id", help="Optional stable workflow identifier (requires --db)")
    args = parser.parse_args(argv)
    if args.run_id and not args.db:
        parser.error("--run-id requires --db")
    try:
        registry = research_tool_catalog()
        planner = LLMTaskPlanner(
            OpenAICompatibleProvider(OpenAICompatibleConfig.from_env()),
            registry,
        )
        plan = planner.plan(args.goal)
        workflow = None
        if args.db:
            engine = WorkflowOrchestrator(SqliteStore(args.db), registry)
            workflow = engine.run(plan, run_id=args.run_id)
    except DomainError as exc:
        print(f"Planning rejected: {exc.code}", file=sys.stderr)
        return 2
    print(json.dumps({
        "status": "PLAN_VALIDATED_ONLY" if workflow is None else workflow.status,
        "execution_performed": False,
        "workflow_run_id": None if workflow is None else workflow.run_id,
        "workflow_event_count": None if workflow is None else workflow.event_count,
        "goal": plan.goal,
        "subtasks": [{
            "id": s.subtask_id,
            "tool_id": s.tool_id,
            "arguments": s.arguments(),
            "depends_on": list(s.depends_on),
            "success_condition": s.success_condition,
        } for s in plan.subtasks],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
