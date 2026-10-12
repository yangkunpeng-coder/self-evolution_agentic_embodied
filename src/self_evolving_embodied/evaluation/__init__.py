"""R09a isolated Fake candidate evaluation evidence (non-promotable)."""

from .evaluator import CandidateEvaluator, EvaluationRecord
from .frozen_suite import FROZEN_SUITE_ID

__all__ = ["FROZEN_SUITE_ID", "CandidateEvaluator", "EvaluationRecord"]

# v16: separate independent offline study for Single-Agent workflow candidates.
from .agent_policy_study import (
    HeldoutCase, HeldoutSuite, IndependentPolicyEvaluator, PolicyProfile,
    PolicyStudyRecord, StudyRunner, TrialEvidence,
)
__all__ += ["HeldoutCase", "HeldoutSuite", "IndependentPolicyEvaluator",
            "PolicyProfile", "PolicyStudyRecord", "StudyRunner", "TrialEvidence"]
