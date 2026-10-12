"""Explicit, human-authorized opt-in of verified research policies for read-only Agent studies.

The research release pointer is not a deployment authorization. An independent
trusted application must inject ReviewerAuthority, authenticate a human, and
activate an allowlisted runtime policy. This module NEVER changes device grants,
controllers, physical limits, model weights or the running sensor pipeline.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Sequence

from ..core.canonical_json import canonical_bytes, content_hash
from ..core.contracts import DomainError
from ..core.ids import uid, utc_now
from ..evaluation.agent_policy_study import PolicyProfile, _ident, _load_object
from ..review.service import ReviewerAuthority
from ..storage.sqlite.database import SqliteStore
from .agent_policy_release import ResearchPolicyGovernance

_RUNTIME_SCOPE = "CONTROLLED_READ_ONLY_RESEARCH"

# Deliberately narrow: only knobs with deterministic dev9 runtime effects.
# A policy requesting an unimplemented behavior is REJECTED, not silently ignored.
def _supported(profile: PolicyProfile) -> None:
    if profile.target not in {
        "PLANNING_STRATEGY", "TOOL_SELECTION", "ASSESSMENT_ROUTING",
        "OBSERVATION_ACQUISITION",
    }:
        raise DomainError("RUNTIME_POLICY_TARGET_NOT_IMPLEMENTED")
    # All these declared knobs now have explicit (and bounded) runtime effects.
    # _supported never authorizes physical actions or arbitrary code.



@dataclass(frozen=True)
class RuntimePolicyBinding:
    """Immutable value pinned to the complete Agent task lineage.

    A binding must be loaded from and verified against the activation database;
    constructing this dataclass does NOT constitute a trusted activation.
    """
    activation_id: str
    manifest_hash: str
    profiles: tuple[PolicyProfile, ...]
    scope: str = _RUNTIME_SCOPE

    def get(self, target: str) -> PolicyProfile | None:
        return next((p for p in self.profiles if p.target == target), None)

    def planning_replan_limit(self, requested: int) -> int:
        p = self.get("PLANNING_STRATEGY")
        return min(requested, p.settings["max_replans"]) if p else requested

    @property
    def uncertain_assessment_retry_limit(self) -> int | None:
        p = self.get("ASSESSMENT_ROUTING")
        return p.settings["max_uncertain_assessments"] if p else None

    @property
    def max_observation_age_ms(self) -> int | None:
        profile = self.get("OBSERVATION_ACQUISITION")
        return profile.settings["max_observation_age_ms"] if profile else None

    @property
    def observation_retry_budget(self) -> int | None:
        profile = self.get("OBSERVATION_ACQUISITION")
        return profile.settings["retry_observation_budget"] if profile else None

    @property
    def max_tool_calls(self) -> int | None:
        p = self.get("TOOL_SELECTION")
        return p.settings["max_tool_calls"] if p else None


class RuntimePolicyActivator:
    """Separate opt-in activation audit; never watches research pointers live."""

    def __init__(self, store: SqliteStore, authority: ReviewerAuthority | None = None) -> None:
        self.store = store
        self.authority = authority

    def _verify_human(self, approver_id: str, credential: str) -> None:
        _ident(approver_id, "RUNTIME_POLICY_APPROVER_INVALID")
        if not isinstance(credential, str) or not credential:
            raise DomainError("RUNTIME_POLICY_CREDENTIAL_REQUIRED")
        if self.authority is None:
            raise DomainError("RUNTIME_POLICY_AUTHORITY_REQUIRED")
        try:
            accepted = self.authority.verify(approver_id, credential)
        except Exception as exc:
            raise DomainError("RUNTIME_POLICY_AUTHORITY_UNAVAILABLE") from exc
        if accepted is not True:
            raise DomainError("RUNTIME_POLICY_APPROVER_UNAUTHORIZED")

    def activate(self, targets: Sequence[str], *, approver_id: str,
                 credential: str, rationale: str) -> RuntimePolicyBinding:
        """Import reviewed research pointers into a SEPARATE controlled-read-only scope.

        A credential comes only from the host application; never from the LLM or
        TaskPlan. This function does not make any policy active for a running task.
        """
        self._verify_human(approver_id, credential)
        if not ResearchPolicyGovernance._valid_reason(rationale):
            raise DomainError("RUNTIME_POLICY_RATIONALE_INVALID")
        if not isinstance(targets, (tuple, list)) or not 1 <= len(targets) <= 4:
            raise DomainError("RUNTIME_POLICY_TARGETS_INVALID")
        if len(set(targets)) != len(targets):
            raise DomainError("RUNTIME_POLICY_TARGETS_DUPLICATE")
        with self.store._write() as c:
            entries: list[dict[str, Any]] = []
            for target in sorted(targets):
                _ident(target, "RUNTIME_POLICY_TARGET_INVALID")
                pointer = ResearchPolicyGovernance._pointer(c, target)
                if pointer.policy is None or pointer.active_study_id is None:
                    raise DomainError("RUNTIME_POLICY_RESEARCH_RELEASE_REQUIRED")
                _supported(pointer.policy)
                study = ResearchPolicyGovernance._study(c, pointer.active_study_id)
                review = ResearchPolicyGovernance._read_review(c, pointer.active_study_id)
                if study.report["gate"] != "PASS" or review.choice != "APPROVE_RESEARCH":
                    raise DomainError("RUNTIME_POLICY_RESEARCH_APPROVAL_REQUIRED")
                entries.append({
                    "target": target, "policy": pointer.policy.to_dict(),
                    "policy_hash": pointer.active_policy_hash,
                    "study_id": pointer.active_study_id,
                    "research_revision": pointer.revision,
                    "research_event_hash": pointer.event_head_hash,
                    "research_review_hash": review.body_hash,
                })
            manifest = {"schema_version": 1, "scope": _RUNTIME_SCOPE,
                        "physical_actions_permitted": False,
                        "approver_id": approver_id, "rationale": rationale,
                        "policies": entries}
            identifier = uid()
            encoded = canonical_bytes(manifest).decode("utf-8")
            digest = content_hash(manifest)
            c.execute("INSERT INTO agent_runtime_policy_activations "
                      "(activation_id,manifest_json,manifest_hash,created_at) VALUES (?,?,?,?)",
                      (identifier, encoded, digest, utc_now()))
            return self._read(c, identifier)

    @staticmethod
    def _read(c: sqlite3.Connection, activation_id: str) -> RuntimePolicyBinding:
        _ident(activation_id, "RUNTIME_POLICY_ACTIVATION_INVALID")
        row = c.execute("SELECT manifest_json,manifest_hash FROM agent_runtime_policy_activations "
                        "WHERE activation_id=?", (activation_id,)).fetchone()
        if row is None:
            raise DomainError("RUNTIME_POLICY_ACTIVATION_NOT_FOUND")
        body = _load_object(row["manifest_json"], row["manifest_hash"],
                            "RUNTIME_POLICY_ACTIVATION_TAMPERED")
        if (set(body) != {"schema_version", "scope", "physical_actions_permitted",
                          "approver_id", "rationale", "policies"}
            or body["schema_version"] != 1 or body["scope"] != _RUNTIME_SCOPE
            or body["physical_actions_permitted"] is not False
            or not ResearchPolicyGovernance._valid_reason(body["rationale"])
            or not isinstance(body["policies"], list)
            or not 1 <= len(body["policies"]) <= 4):
            raise DomainError("RUNTIME_POLICY_ACTIVATION_TAMPERED")
        _ident(body["approver_id"], "RUNTIME_POLICY_ACTIVATION_TAMPERED")
        profiles: list[PolicyProfile] = []
        last_target = ""
        for entry in body["policies"]:
            if not isinstance(entry, dict) or set(entry) != {
                "target", "policy", "policy_hash", "study_id", "research_revision",
                "research_event_hash", "research_review_hash"}:
                raise DomainError("RUNTIME_POLICY_ACTIVATION_TAMPERED")
            target = entry["target"]
            if not isinstance(target, str) or target <= last_target:
                raise DomainError("RUNTIME_POLICY_ACTIVATION_TAMPERED")
            last_target = target
            study = ResearchPolicyGovernance._study(c, entry["study_id"])
            review = ResearchPolicyGovernance._read_review(c, entry["study_id"])
            profile = ResearchPolicyGovernance._policy_from_study(study)
            _supported(profile)
            if (profile.target != target or entry["policy"] != profile.to_dict()
                or entry["policy_hash"] != profile.digest
                or review.choice != "APPROVE_RESEARCH" or study.report["gate"] != "PASS"
                or entry["research_review_hash"] != review.body_hash
                or type(entry["research_revision"]) is not int
                or entry["research_revision"] < 1
                or not isinstance(entry["research_event_hash"], str)
                or len(entry["research_event_hash"]) != 64):
                raise DomainError("RUNTIME_POLICY_ACTIVATION_TAMPERED")
            # Historical pointer proof: may no longer be current after rollback.
            history = c.execute("SELECT body_hash,policy_hash,study_id FROM "
                                "agent_policy_release_events WHERE target=? AND revision=?",
                                (target, entry["research_revision"])).fetchone()
            if (history is None or history["body_hash"] != entry["research_event_hash"]
                or history["policy_hash"] != profile.digest
                or history["study_id"] != entry["study_id"]):
                raise DomainError("RUNTIME_POLICY_RELEASE_PROVENANCE_INVALID")
            profiles.append(profile)
        return RuntimePolicyBinding(activation_id, row["manifest_hash"], tuple(profiles))

    def read(self, activation_id: str) -> RuntimePolicyBinding:
        with closing(self.store._connect()) as c:
            return self._read(c, activation_id)

    def validate_binding(self, binding: RuntimePolicyBinding) -> None:
        if not isinstance(binding, RuntimePolicyBinding):
            raise DomainError("INVALID_RUNTIME_POLICY_BINDING")
        trusted = self.read(binding.activation_id)
        if trusted != binding:
            raise DomainError("RUNTIME_POLICY_BINDING_MISMATCH")
