"""Trusted R0-only release governance and fail-closed rollback requests."""

from .release_management import ActivationAttempt, ReleaseManager, ReleasePointer, StagedRelease
from .release_resolver import ReleaseResolver
from .rollback import RollbackAttempt, RollbackManager

__all__ = [
    "ActivationAttempt",
    "ReleaseManager",
    "ReleasePointer",
    "ReleaseResolver",
    "RollbackAttempt",
    "RollbackManager",
    "StagedRelease",
]

# v16 research-only, NOT the legacy executable release manager.
from .agent_policy_release import ResearchPolicyGovernance, ResearchPolicyPointer, ResearchReview
__all__ += ["ResearchPolicyGovernance", "ResearchPolicyPointer", "ResearchReview"]

# v17 explicit opt-in controlled research-runtime activation.
from .runtime_policy import RuntimePolicyActivator, RuntimePolicyBinding
__all__ += ["RuntimePolicyActivator", "RuntimePolicyBinding"]
