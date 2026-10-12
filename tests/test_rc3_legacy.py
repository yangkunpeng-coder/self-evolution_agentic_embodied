"""Preserve historical Fake-only memory/experience compatibility, no hardware."""
import tempfile

import pytest

from self_evolving_embodied.adapters.fake.gateway import FakeGateway
from self_evolving_embodied.core.contracts import Postcondition, Status, TaskContract
from self_evolving_embodied.diagnosis.analyzer import FailureDiagnoser
from self_evolving_embodied.evaluation import CandidateEvaluator
from self_evolving_embodied.evolution import EvolutionCandidateGenerator
from self_evolving_embodied.experience.collector import ExperienceCollector
from self_evolving_embodied.memory.store import MemoryTrust, SqliteMemory
from self_evolving_embodied.runtime.orchestrator import FakeHarness
from self_evolving_embodied.runtime.planning import FakePlanner
from self_evolving_embodied.storage.sqlite.database import SqliteStore


@pytest.mark.parametrize('scenario,expected', [('success', Status.SUCCEEDED), ('goal_fail', Status.FAILED)])
def test_legacy_fake_memory_experience_and_diagnosis(tmp_path, monkeypatch, scenario, expected):
    # Keep the legacy evaluator's TemporaryDirectory inside pytest's writable fixture.
    monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))
    store = SqliteStore(tmp_path/'legacy.db')
    task = TaskContract('legacy-test', Postcondition('goal', 'goal_reached', True), ('fake.move',))
    outcome = FakeHarness(store, FakeGateway(scenario), FakePlanner()).run(task)
    assert outcome.status is expected
    memory = SqliteMemory(store)
    memory.project_pending()
    assert memory.project_pending() == 0
    records = memory.search(task_id=task.task_id, release_id=task.release_id)
    assert all(record.trust_level is MemoryTrust.VERIFIED for record in records)
    if expected is Status.SUCCEEDED:
        assert records
    collector = ExperienceCollector(store)
    assert collector.collect_terminal() == 1
    assert collector.collect_terminal() == 0
    assert collector.get(task.task_id, release_id=task.release_id).terminal_status is expected
    diagnoser = FailureDiagnoser(store)
    assert diagnoser.diagnose_pending() == (1 if expected is Status.FAILED else 0)
    assert diagnoser.diagnose_pending() == 0
    if expected is Status.FAILED:
        generator = EvolutionCandidateGenerator(store)
        assert generator.generate_pending() == 1
        assert generator.generate_pending() == 0
        candidate = generator.get(task.task_id, release_id=task.release_id)
        evaluation = CandidateEvaluator(store).evaluate(candidate.candidate_id)
        assert evaluation.report['gate_status'] == 'BLOCKED'
        assert evaluation.report['promotion_eligible'] is False
