"""Exercise existing offline governance contracts using synthetic test evidence.

These fixtures validate software gates only; they are not datasets/benchmarks
and do not establish any policy improvement in a robot or model.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import replace

import pytest

from self_evolving_embodied import DomainError, FrameworkBuilder, FrameworkConfig
from self_evolving_embodied.agent import AgentAction, AgentDecision
from self_evolving_embodied.evaluation import (
    HeldoutCase, HeldoutSuite, IndependentPolicyEvaluator, PolicyProfile, TrialEvidence,
)
from self_evolving_embodied.evolution import WorkflowEvolutionCycle
from self_evolving_embodied.governance import ResearchPolicyGovernance, RuntimePolicyActivator
from test_rc_acceptance import Brain, SPEC
from test_rc3_integration import _plain_framework, _plan


class ContinuingBrain(Brain):
    def decide(self, context):
        return AgentDecision(AgentAction.CONTINUE, 'Continue read-only task')


class StoppingBrain(Brain):
    def decide(self, context):
        return AgentDecision(AgentAction.STOP, 'Stop known failed read-only task')


class TestAuthority:
    def verify(self, reviewer_id, credential):
        return reviewer_id == 'rc3-reviewer' and credential == 'local-test-credential'


def _study(store, run_id, baseline, candidate, *, improves=True):
    cycle = WorkflowEvolutionCycle(store)
    assert cycle.run_cycle() == {'experiences': 1, 'diagnoses': 1, 'proposals': 1}
    assert cycle.run_cycle() == {'experiences': 0, 'diagnoses': 0, 'proposals': 0}
    experience = cycle.get_experience(run_id)
    assert experience.content['ground_truth_available'] is False
    assert cycle.get_proposal(run_id).target.value == candidate.target
    evaluator = IndependentPolicyEvaluator(store)
    suite = HeldoutSuite('rc3-synthetic-suite', 'test-only', 'isolated-test-ids', 'stub-oracle', tuple(
        HeldoutCase(f'case-{i}', f'isolated-{i}', 'synthetic-contract-test', f'stub-oracle-{i}')
        for i in range(8)))
    evaluator.freeze_suite(suite)
    class Runner:
        evaluator_id = 'rc3-stub-evaluator'
        operator_id = 'rc3-test-operator'
        def evaluate_case(self, case, policy):
            success = (improves and policy.digest == candidate.digest) or int(case.case_id[-1]) < 4
            return TrialEvidence(case.case_id, policy.digest, case.oracle_ref, success, success, 0, 10)
    record = evaluator.evaluate(proposal_run_id=run_id, suite_id=suite.suite_id,
                                baseline=baseline, candidate=candidate, runner=Runner())
    assert evaluator.get_study(record.study_id).report_hash == record.report_hash
    return record


def _approved_binding(store, study):
    governance = ResearchPolicyGovernance(store, TestAuthority())
    kwargs = dict(reviewer_id='rc3-reviewer', credential='local-test-credential')
    with pytest.raises(DomainError, match='RESEARCH_REVIEW_REQUIRED'):
        governance.promote(study.study_id, expected_revision=0, **kwargs)
    governance.review(study.study_id, choice='APPROVE_RESEARCH',
                      rationale='Synthetic gate test approved by separate test reviewer', **kwargs)
    pointer = governance.promote(study.study_id, expected_revision=0, **kwargs)
    assert pointer.revision == 1 and pointer.scope == 'OFFLINE_RESEARCH_ONLY'
    activator = RuntimePolicyActivator(store, TestAuthority())
    binding = activator.activate([pointer.target], approver_id='rc3-reviewer',
                                credential='local-test-credential', rationale='Synthetic controlled read-only activation test')
    activator.validate_binding(binding)
    return governance, binding


def test_complete_workflow_evaluation_review_release_rollback_and_pin(tmp_path):
    path = tmp_path/'governance.db'
    with _plain_framework(path, ContinuingBrain) as app:
        run = app.run_instruction('Inspect synthetic object')
        store = app.components.store
    baseline = PolicyProfile('ASSESSMENT_ROUTING', {'prefer_independent_vision': False, 'max_uncertain_assessments': 1})
    candidate = PolicyProfile('ASSESSMENT_ROUTING', {'prefer_independent_vision': True, 'max_uncertain_assessments': 2})
    study = _study(store, run.root_run_id, baseline, candidate)
    assert study.report['gate'] == 'PASS' and study.report['production_release_authorized'] is False
    with pytest.raises(DomainError, match='RESEARCH_HUMAN_AUTHORITY_REQUIRED'):
        ResearchPolicyGovernance(store).review(study.study_id, reviewer_id='rc3-reviewer',
            credential='local-test-credential', choice='APPROVE_RESEARCH', rationale='A separate reviewer is required')
    governance, binding = _approved_binding(store, study)
    with pytest.raises(DomainError, match='RUNTIME_POLICY_BINDING_MISMATCH'):
        RuntimePolicyActivator(store).validate_binding(replace(binding, profiles=(baseline,)))
    rolled_back = governance.rollback(candidate.target, reviewer_id='rc3-reviewer',
        credential='local-test-credential', rationale='Rollback synthetic contract test', expected_revision=1)
    assert rolled_back.revision == 2 and rolled_back.active_policy_hash is None
    # A durable old activation remains pinned despite pointer rollback.
    RuntimePolicyActivator(store).validate_binding(binding)
    with pytest.raises(DomainError, match='RUNTIME_POLICY_RESEARCH_RELEASE_REQUIRED'):
        RuntimePolicyActivator(store, TestAuthority()).activate([candidate.target], approver_id='rc3-reviewer',
            credential='local-test-credential', rationale='New activation after rollback is forbidden')
    b = FrameworkBuilder(FrameworkConfig(path), brain_factory=ContinuingBrain)
    b.add_execution_tool(SPEC, factory=lambda _r: type('Tool', (), {'invoke': lambda _s, _a: {'tool_report_available': True}})())
    b.add_evidence_policy('tool_report_available', lambda _a, out: out.get('tool_report_available') is True)
    b.bind_runtime_policy(binding)
    with b.build() as app:
        task = app.run_instruction('Future synthetic task')
        assert task.status == 'EXECUTION_WORKFLOW_COMPLETED'
        assert app.components.workflow.workflows.policy_pin(task.root_run_id) == (binding.activation_id, binding.manifest_hash)
    with _plain_framework(path, ContinuingBrain) as unpinned:
        with pytest.raises(DomainError, match='RUNTIME_POLICY_PIN_REQUIRED_OR_MISMATCHED'):
            unpinned.resume_instruction(task.root_run_id)
    with closing(store._connect()) as db:
        assert db.execute('PRAGMA foreign_key_check').fetchone() is None
        assert 'local-test-credential' not in '\n'.join(db.iterdump())


def test_blocked_offline_evaluation_cannot_be_approved(tmp_path):
    with _plain_framework(tmp_path/'blocked.db', ContinuingBrain) as app:
        run = app.run_instruction('Inspect')
        store = app.components.store
    baseline = PolicyProfile('ASSESSMENT_ROUTING', {'prefer_independent_vision': False, 'max_uncertain_assessments': 1})
    candidate = PolicyProfile('ASSESSMENT_ROUTING', {'prefer_independent_vision': True, 'max_uncertain_assessments': 2})
    study = _study(store, run.root_run_id, baseline, candidate, improves=False)
    assert study.report['gate'] == 'BLOCKED'
    with pytest.raises(DomainError, match='RESEARCH_STUDY_GATE_BLOCKED'):
        ResearchPolicyGovernance(store, TestAuthority()).review(study.study_id, reviewer_id='rc3-reviewer',
            credential='local-test-credential', choice='APPROVE_RESEARCH', rationale='Blocked contract test cannot be approved')


def test_runtime_tool_budget_enforced_at_workflow_dispatch(tmp_path):
    class FailingTool:
        def invoke(self, args):
            raise RuntimeError('Known read-only failure')
    with _plain_framework(tmp_path/'budget.db', StoppingBrain, FailingTool()) as app:
        run = app.run_instruction('Inspect')
        store = app.components.store
    baseline = PolicyProfile('TOOL_SELECTION', {'prefer_read_only_first': False, 'max_tool_calls': 10})
    candidate = PolicyProfile('TOOL_SELECTION', {'prefer_read_only_first': True, 'max_tool_calls': 1})
    study = _study(store, run.root_run_id, baseline, candidate)
    _, binding = _approved_binding(store, study)
    with _plain_framework(tmp_path/'registry.db', ContinuingBrain) as app:
        workflow = app.components.workflow
        # Use the governed store, with identical local read-only registry.
        from self_evolving_embodied.runtime.workflow_orchestrator import WorkflowOrchestrator
        engine = WorkflowOrchestrator(store, workflow.registry, workflow.evidence_policies)
        plan = _plan(workflow.registry, 'Inspect twice', 2)
        result = engine.run(plan, policy_pin=(binding.activation_id, binding.manifest_hash))
        assert result.status == 'WAITING_POLICY'
        assert list(result.subtask_states.values()) == ['SUCCEEDED', 'PENDING']
        assert engine.resume(result.run_id).status == 'WAITING_POLICY'
        # Directly resuming a successor must not reset the approved call budget.
        with store.worker_session():
            engine.workflows.create_replan(result.run_id, 'budget-successor',
                _plan(workflow.registry, 'Inspect twice'), engine.catalog_hash(),
                policy_pin=(binding.activation_id, binding.manifest_hash))
        successor = engine.resume('budget-successor')
        assert successor.status == 'WAITING_POLICY'
        assert list(successor.subtask_states.values()) == ['PENDING']
