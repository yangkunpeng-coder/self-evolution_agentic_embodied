"""Frozen synthetic Fake regression fixtures, not real robot held-out data.

Case families intentionally overlap the Fake training/development scenarios;
these fixtures are held apart by case/task IDs and persistence, but MUST NOT
be represented as an independent embodied benchmark or a causal improvement test.
"""

from __future__ import annotations

from typing import Any

FROZEN_SUITE_ID = "R09-FAKE-FROZEN-REGRESSION-V1"


def frozen_suite() -> dict[str, Any]:
    """Return a fresh copy; the source definition and SQLite record are immutable."""
    return {
        "schema_version": 1,
        "suite_id": FROZEN_SUITE_ID,
        "suite_kind": "FROZEN_SYNTHETIC_FAKE_REGRESSION_PROXY",
        "release_id": "R0-FAKE",
        "independent_real_embodied_heldout": False,
        "training_scenario_family_overlap": True,
        "real_robot_validation": False,
        "cases": [
            {"case_id": "FROZEN-01", "scenario": "success", "tool_id": "fake.move",
             "expected_status": "SUCCEEDED", "expected_invocations": 1},
            {"case_id": "FROZEN-02", "scenario": "goal_fail", "tool_id": "fake.move",
             "expected_status": "FAILED", "expected_invocations": 1},
            {"case_id": "FROZEN-03", "scenario": "missing_after", "tool_id": "fake.move",
             "expected_status": "FAILED", "expected_invocations": 1},
            {"case_id": "FROZEN-04", "scenario": "unauthorized", "tool_id": "unapproved.tool",
             "expected_status": "FAILED", "expected_invocations": 0},
            {"case_id": "FROZEN-05", "scenario": "unknown_stop", "tool_id": "fake.move",
             "expected_status": "HUMAN_REVIEW", "expected_invocations": 1},
        ],
    }
