"""R08 evidence-gated, offline, non-executable candidate generation."""

from .candidates import CandidateRecord, EvolutionCandidateGenerator, EvolutionTarget

from .workflow_cycle import (
    EvolutionPolicyTarget, WorkflowDiagnosisKind, WorkflowDiagnosisRecord,
    WorkflowEvolutionCycle, WorkflowEvolutionRecord, WorkflowProposalRecord,
)

__all__ = [
    "CandidateRecord", "EvolutionCandidateGenerator", "EvolutionTarget",
    "EvolutionPolicyTarget", "WorkflowDiagnosisKind", "WorkflowDiagnosisRecord",
    "WorkflowEvolutionCycle", "WorkflowEvolutionRecord", "WorkflowProposalRecord",
]
