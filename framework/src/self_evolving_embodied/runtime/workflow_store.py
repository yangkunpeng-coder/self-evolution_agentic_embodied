"""Event-linked, fenced SQLite persistence for multi-subtask task plans.

These tables share the existing SqliteStore connection and worker lease, but
are deliberately separate from the legacy single-step task state machine.
"""
from __future__ import annotations

import json
from contextlib import closing, contextmanager
from dataclasses import dataclass
from hashlib import sha256
from collections.abc import Iterator
from typing import Any

from ..core.contracts import DomainError
from ..core.ids import utc_now
from ..planning.contracts import TaskPlan
from ..storage.sqlite.database import SqliteStore
from ..tools.contracts import _bounded_json


@dataclass(frozen=True)
class WorkflowSnapshot:
    run_id: str
    status: str
    state_version: int
    subtask_states: dict[str, str]
    failure_code: str | None
    plan_hash: str
    event_count: int


def _encoded(value: Any) -> str:
    # The LLM plan limit is 32 KiB; individual tool inputs/results are 8 KiB.
    # Supports finite floating-point arguments unlike the narrow legacy digest.
    try:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                             allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise DomainError("INVALID_WORKFLOW_JSON") from None
    if len(encoded.encode("utf-8")) > 65_536:
        raise DomainError("WORKFLOW_PAYLOAD_TOO_LARGE")
    return encoded


def _digest(value: Any) -> str:
    return sha256(_encoded(value).encode("utf-8")).hexdigest()


class WorkflowStore:
    """State transitions are transactional, event-linked, and fenced.

    Mutating methods require the caller to hold ``SqliteStore.worker_session``.
    """

    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    @contextmanager
    def _fenced_write(self) -> Iterator[Any]:
        # Legacy SqliteStore also supports historical unfenced administration.
        # Agent workflows explicitly require a live worker lease for ALL writes.
        self.store.active_fencing_token()
        with self.store._write() as c:
            yield c

    @staticmethod
    def _plan_body(plan: TaskPlan) -> dict[str, Any]:
        return {
            "goal": plan.goal,
            "subtasks": [
                {
                    "id": s.subtask_id, "tool_id": s.tool_id,
                    "arguments": s.arguments(), "depends_on": list(s.depends_on),
                    "success_condition": s.success_condition,
                }
                for s in plan.subtasks
            ],
        }

    def create(self, run_id: str, plan: TaskPlan, catalog_hash: str,
               policy_pin: tuple[str, str] | None = None,
               routing_manifest: dict[str, Any] | None = None) -> None:
        with self._fenced_write() as c:
            self._create(c, run_id, plan, catalog_hash, policy_pin, routing_manifest)

    def create_replan(self, run_id: str, next_run_id: str, plan: TaskPlan,
                      catalog_hash: str, policy_pin: tuple[str, str] | None = None,
                      routing_manifest: dict[str, Any] | None = None) -> None:
        """Persist successor and supersede parent in ONE transaction, without dispatch."""
        with self._fenced_write() as c:
            self._create(c, next_run_id, plan, catalog_hash, policy_pin, routing_manifest)
            self._mark_replanned(c, run_id, next_run_id)

    def _create(self, c: Any, run_id: str, plan: TaskPlan, catalog_hash: str,
                policy_pin: tuple[str, str] | None,
                routing_manifest: dict[str, Any] | None) -> None:
        if not isinstance(run_id, str) or not 1 <= len(run_id) <= 128:
            raise DomainError("INVALID_WORKFLOW_ID")
        body = self._plan_body(plan)
        encoded = _encoded(body)
        digest = _digest(body)
        now = utc_now()
        existing = c.execute(
            "SELECT plan_hash,catalog_hash FROM agent_workflows WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if existing:
            if existing["plan_hash"] != digest or existing["catalog_hash"] != catalog_hash:
                raise DomainError("WORKFLOW_ID_CONFLICT")
            pinned = self._read_policy_pin(c, run_id)
            if pinned != policy_pin:
                raise DomainError("WORKFLOW_POLICY_PIN_CONFLICT")
            previous = self._read_routing_pin(c, run_id)
            if previous != (_digest(routing_manifest) if routing_manifest is not None else None):
                raise DomainError("WORKFLOW_ROUTING_PIN_CONFLICT")
            return
        c.execute(
            "INSERT INTO agent_workflows "
            "(run_id,plan_json,plan_hash,catalog_hash,status,state_version,"
            "event_seq,last_event_hash,failure_code,created_at,updated_at) "
            "VALUES (?,?,?,?,?,0,0,'',NULL,?,?)",
            (run_id, encoded, digest, catalog_hash, "PLANNED", now, now),
        )
        for index, s in enumerate(plan.subtasks):
            c.execute(
                "INSERT INTO agent_subtasks "
                "(run_id,subtask_id,ordinal,tool_id,arguments_json,depends_on_json,"
                "success_condition,status,invocation_id,result_json,failure_code) "
                "VALUES (?,?,?,?,?,?,?,?,NULL,NULL,NULL)",
                (run_id, s.subtask_id, index, s.tool_id,
                 s.arguments_json, json.dumps(list(s.depends_on)),
                 s.success_condition, "PENDING"),
            )
        self._append(c, run_id, "PLAN_ACCEPTED", {"plan_hash": digest,
                                                  "catalog_hash": catalog_hash})
        if policy_pin is not None:
            if (not isinstance(policy_pin, tuple) or len(policy_pin) != 2
                or not all(isinstance(v, str) for v in policy_pin)):
                raise DomainError("INVALID_WORKFLOW_POLICY_PIN")
            activation_id, manifest_hash = policy_pin
            row = c.execute("SELECT manifest_hash FROM agent_runtime_policy_activations "
                            "WHERE activation_id=?", (activation_id,)).fetchone()
            if row is None or row["manifest_hash"] != manifest_hash:
                raise DomainError("WORKFLOW_POLICY_ACTIVATION_INVALID")
            c.execute("INSERT INTO agent_runtime_policy_pins "
                      "(run_id,activation_id,manifest_hash,created_at) VALUES (?,?,?,?)",
                      (run_id, activation_id, manifest_hash, now))
            self._append(c, run_id, "RUNTIME_POLICY_PINNED", {
                "activation_id": activation_id, "manifest_hash": manifest_hash,
                "scope": "CONTROLLED_READ_ONLY_RESEARCH"})
        if routing_manifest is not None:
            if not isinstance(routing_manifest, dict):
                raise DomainError("INVALID_WORKFLOW_ROUTING_PIN")
            encoded_routing = _encoded(routing_manifest)
            routing_hash = _digest(routing_manifest)
            c.execute("INSERT INTO agent_runtime_routing_pins "
                      "(run_id,routing_json,routing_hash,created_at) VALUES (?,?,?,?)",
                      (run_id, encoded_routing, routing_hash, now))
            self._append(c, run_id, "RUNTIME_ROUTING_PINNED", {"routing_hash": routing_hash})

    @staticmethod
    def _read_routing_pin(c: Any, run_id: str) -> str | None:
        row = c.execute("SELECT routing_json,routing_hash FROM agent_runtime_routing_pins "
                        "WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        try:
            body = json.loads(row["routing_json"])
        except (ValueError, TypeError):
            raise DomainError("WORKFLOW_ROUTING_PIN_TAMPERED") from None
        if (row["routing_json"] != _encoded(body)
                or row["routing_hash"] != _digest(body)):
            raise DomainError("WORKFLOW_ROUTING_PIN_TAMPERED")
        return row["routing_hash"]

    def routing_hash(self, run_id: str) -> str | None:
        with closing(self.store._connect()) as c:
            workflow = self._workflow(c, run_id)
            self._check_chain(c, workflow)
            digest = self._read_routing_pin(c, run_id)
            events = c.execute("SELECT payload_json FROM agent_workflow_events "
                               "WHERE run_id=? AND kind='RUNTIME_ROUTING_PINNED'",
                               (run_id,)).fetchall()
            if (digest is None and events) or (digest is not None and
                (len(events) != 1 or json.loads(events[0]["payload_json"]) != {"routing_hash": digest})):
                raise DomainError("WORKFLOW_ROUTING_PIN_TAMPERED")
            return digest

    @staticmethod
    def _read_policy_pin(c: Any, run_id: str) -> tuple[str, str] | None:
        row = c.execute("SELECT p.activation_id,p.manifest_hash,a.manifest_hash "
                        "AS active_hash FROM agent_runtime_policy_pins p JOIN "
                        "agent_runtime_policy_activations a USING (activation_id) "
                        "WHERE p.run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        if row["manifest_hash"] != row["active_hash"]:
            raise DomainError("WORKFLOW_POLICY_PIN_TAMPERED")
        return row["activation_id"], row["manifest_hash"]

    def policy_pin(self, run_id: str) -> tuple[str, str] | None:
        with closing(self.store._connect()) as c:
            workflow = self._workflow(c, run_id)
            self._check_chain(c, workflow)
            result = self._read_policy_pin(c, run_id)
            events = c.execute("SELECT payload_json FROM agent_workflow_events "
                               "WHERE run_id=? AND kind='RUNTIME_POLICY_PINNED'", (run_id,)).fetchall()
            if result is None and events:
                raise DomainError("WORKFLOW_POLICY_PIN_TAMPERED")
            if result is not None:
                if len(events) != 1:
                    raise DomainError("WORKFLOW_POLICY_PIN_TAMPERED")
                payload = json.loads(events[0]["payload_json"])
                if payload != {"activation_id": result[0], "manifest_hash": result[1],
                               "scope": "CONTROLLED_READ_ONLY_RESEARCH"}:
                    raise DomainError("WORKFLOW_POLICY_PIN_TAMPERED")
            return result

    def dispatched_count(self, root_run_id: str) -> int:
        """Count all dispatched subtasks across the recorded replan chain."""
        current = root_run_id
        seen: set[str] = set()
        total = 0
        with closing(self.store._connect()) as c:
            while True:
                if current in seen or len(seen) > 32:
                    raise DomainError("WORKFLOW_POLICY_REPLAN_CYCLE")
                seen.add(current)
                row = self._workflow(c, current)
                self._check_chain(c, row)
                total += c.execute("SELECT COUNT(*) FROM agent_subtasks "
                                   "WHERE run_id=? AND status != 'PENDING'", (current,)).fetchone()[0]
                links = c.execute("SELECT payload_json FROM agent_workflow_events "
                                  "WHERE run_id=? AND kind='AGENT_REPLAN_LINKED'", (current,)).fetchall()
                if not links:
                    return total
                if len(links) != 1:
                    raise DomainError("WORKFLOW_POLICY_DUPLICATE_REPLAN")
                current = json.loads(links[0]["payload_json"])["next_run_id"]

    def lineage_root(self, run_id: str) -> str:
        """Resolve durable replan ancestry for dispatch-boundary policy budgets."""
        with closing(self.store._connect()) as c:
            current = run_id
            seen: set[str] = set()
            links = c.execute("SELECT run_id,payload_json FROM agent_workflow_events "
                              "WHERE kind='AGENT_REPLAN_LINKED'").fetchall()
            while True:
                if current in seen or len(seen) >= 32:
                    raise DomainError("WORKFLOW_POLICY_REPLAN_CYCLE")
                seen.add(current)
                self._check_chain(c, self._workflow(c, current))
                parents = [link["run_id"] for link in links
                           if json.loads(link["payload_json"]).get("next_run_id") == current]
                if not parents:
                    return current
                if len(parents) != 1:
                    raise DomainError("WORKFLOW_POLICY_DUPLICATE_REPLAN")
                current = parents[0]

    @staticmethod
    def _workflow(c: Any, run_id: str) -> Any:
        row = c.execute("SELECT * FROM agent_workflows WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise DomainError("WORKFLOW_NOT_FOUND")
        return row

    @classmethod
    def _check_chain(cls, c: Any, row: Any) -> None:
        previous = ""
        events = c.execute(
            "SELECT seq,kind,payload_json,previous_hash,event_hash "
            "FROM agent_workflow_events WHERE run_id=? ORDER BY seq", (row["run_id"],)
        ).fetchall()
        for seq, event in enumerate(events, 1):
            try:
                payload = json.loads(event["payload_json"])
                actual = _digest({"run_id": row["run_id"], "seq": seq,
                                       "kind": event["kind"], "payload": payload,
                                       "previous_hash": previous})
            except (TypeError, ValueError, DomainError):
                raise DomainError("WORKFLOW_EVENT_CORRUPTED") from None
            if (event["seq"] != seq or event["previous_hash"] != previous
                or event["event_hash"] != actual):
                raise DomainError("WORKFLOW_EVENT_CORRUPTED")
            previous = actual
        if row["event_seq"] != len(events) or row["last_event_hash"] != previous:
            raise DomainError("WORKFLOW_EVENT_CORRUPTED")

    @classmethod
    def _append(cls, c: Any, run_id: str, kind: str, payload: dict[str, Any]) -> None:
        row = cls._workflow(c, run_id)
        cls._check_chain(c, row)
        seq = row["event_seq"] + 1
        frozen = _bounded_json(payload)
        event_hash = _digest({"run_id": run_id, "seq": seq, "kind": kind,
                                   "payload": frozen, "previous_hash": row["last_event_hash"]})
        c.execute(
            "INSERT INTO agent_workflow_events "
            "(run_id,seq,kind,payload_json,previous_hash,event_hash,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (run_id, seq, kind, _encoded(frozen),
             row["last_event_hash"], event_hash, utc_now()),
        )
        c.execute(
            "UPDATE agent_workflows SET event_seq=?,last_event_hash=?,"
            "state_version=state_version+1,updated_at=? WHERE run_id=?",
            (seq, event_hash, utc_now(), run_id),
        )

    def load_plan(self, run_id: str) -> tuple[dict[str, Any], str]:
        with closing(self.store._connect()) as c:
            row = self._workflow(c, run_id)
            self._check_chain(c, row)
            try:
                body = json.loads(row["plan_json"])
                if _digest(body) != row["plan_hash"]:
                    raise DomainError("WORKFLOW_PLAN_CORRUPTED")
            except (ValueError, TypeError):
                raise DomainError("WORKFLOW_PLAN_CORRUPTED") from None
            return body, row["catalog_hash"]

    def snapshot(self, run_id: str) -> WorkflowSnapshot:
        with closing(self.store._connect()) as c:
            row = self._workflow(c, run_id)
            self._check_chain(c, row)
            states = c.execute(
                "SELECT subtask_id,status FROM agent_subtasks WHERE run_id=? ORDER BY ordinal",
                (run_id,),
            ).fetchall()
            return WorkflowSnapshot(run_id, row["status"], row["state_version"],
                                    {s["subtask_id"]: s["status"] for s in states},
                                    row["failure_code"], row["plan_hash"], row["event_seq"])

    def wait(self, run_id: str, status: str, code: str) -> None:
        if status not in {"WAITING_TOOL", "WAITING_VERIFIER", "WAITING_EXECUTION_BRIDGE",
                           "WAITING_ASSESSMENT", "WAITING_POLICY"}:
            raise DomainError("INVALID_WORKFLOW_STATE")
        with self._fenced_write() as c:
            row = self._workflow(c, run_id)
            if row["status"] in {"SUCCEEDED", "BLOCKED", "HUMAN_REVIEW"}:
                raise DomainError("WORKFLOW_TERMINAL")
            if row["status"] == status and row["failure_code"] == code:
                return
            c.execute("UPDATE agent_workflows SET status=?,failure_code=? WHERE run_id=?",
                      (status, code, run_id))
            self._append(c, run_id, status, {"reason": code})

    def begin(self, run_id: str, subtask_id: str, invocation_id: str,
              effect_intent: dict[str, Any] | None = None) -> None:
        with self._fenced_write() as c:
            row = self._workflow(c, run_id)
            if row["status"] in {"SUCCEEDED", "BLOCKED", "HUMAN_REVIEW"}:
                raise DomainError("WORKFLOW_TERMINAL")
            step = c.execute(
                "SELECT * FROM agent_subtasks WHERE run_id=? AND subtask_id=?",
                (run_id, subtask_id),
            ).fetchone()
            if step is None or step["status"] != "PENDING":
                raise DomainError("SUBTASK_NOT_READY")
            for parent in json.loads(step["depends_on_json"]):
                dependency = c.execute(
                    "SELECT status FROM agent_subtasks WHERE run_id=? AND subtask_id=?",
                    (run_id, parent),
                ).fetchone()
                if dependency is None or dependency["status"] != "SUCCEEDED":
                    raise DomainError("SUBTASK_DEPENDENCY_UNSATISFIED")
            if effect_intent is not None:
                resource = effect_intent["resource"]
                tool_id = effect_intent["tool_id"]
                digest = effect_intent["arguments_hash"]
                token = effect_intent["fencing_token"]
                if (tool_id != step["tool_id"] or
                    digest != _digest(json.loads(step["arguments_json"])) or
                    token != c.execute(
                        "SELECT fence_token FROM worker_lease WHERE singleton=1"
                    ).fetchone()[0]):
                    raise DomainError("EFFECTFUL_AUTHORIZATION_MISMATCH")
                # Reject a quarantined / possibly in-flight resource even when
                # that invocation belongs to a different workflow or legacy task.
                self.store._ensure_resource_clear(c, resource)
                ambiguous = c.execute(
                    "SELECT 1 FROM agent_effectful_invocations "
                    "WHERE resource=? AND status IN ('DISPATCHING','QUARANTINED') LIMIT 1",
                    (resource,),
                ).fetchone()
                if ambiguous is not None:
                    raise DomainError("RESOURCE_IN_FLIGHT_OR_QUARANTINED")
                c.execute(
                    "INSERT INTO agent_effectful_invocations "
                    "(invocation_id,run_id,subtask_id,tool_id,resource,arguments_hash,"
                    "fencing_token,status,created_at) "
                    "VALUES (?,?,?,?,?,?,?,'DISPATCHING',?)",
                    (invocation_id, run_id, subtask_id, tool_id, resource,
                     digest, token, utc_now()),
                )
            c.execute("UPDATE agent_subtasks SET status='DISPATCHING',invocation_id=? "
                      "WHERE run_id=? AND subtask_id=?", (invocation_id, run_id, subtask_id))
            c.execute("UPDATE agent_workflows SET status='RUNNING',failure_code=NULL "
                      "WHERE run_id=?", (run_id,))
            self._append(c, run_id, "SUBTASK_INVOCATION_INTENT",
                         {"subtask_id": subtask_id, "invocation_id": invocation_id,
                          "tool_id": step["tool_id"],
                          "arguments_hash": _digest(json.loads(step["arguments_json"])),
                          "effectful_resource": (effect_intent or {}).get("resource"),
                          "fencing_token": (effect_intent or {}).get("fencing_token")})

    def finish(self, run_id: str, subtask_id: str, invocation_id: str,
               result: dict[str, Any] | None, accepted: bool, failure_code: str | None) -> None:
        with self._fenced_write() as c:
            row = c.execute(
                "SELECT status,invocation_id FROM agent_subtasks "
                "WHERE run_id=? AND subtask_id=?", (run_id, subtask_id),
            ).fetchone()
            if row is None or row["status"] != "DISPATCHING" or row["invocation_id"] != invocation_id:
                raise DomainError("STALE_SUBTASK_RESULT")
            serialized = None
            result_digest = None
            if result is not None:
                frozen = _bounded_json(result)
                serialized = _encoded(frozen)
                result_digest = _digest(frozen)
            new_status = "SUCCEEDED" if accepted else "BLOCKED"
            c.execute(
                "UPDATE agent_effectful_invocations SET status=? WHERE invocation_id=?",
                ("SUCCEEDED" if accepted else "QUARANTINED", invocation_id),
            )
            c.execute("UPDATE agent_subtasks SET status=?,result_json=?,failure_code=? "
                      "WHERE run_id=? AND subtask_id=?",
                      (new_status, serialized, failure_code, run_id, subtask_id))
            c.execute("UPDATE agent_workflows SET status=?,failure_code=? WHERE run_id=?",
                      ("RUNNING" if accepted else "BLOCKED", failure_code, run_id))
            self._append(c, run_id, "SUBTASK_VERIFIED" if accepted else "SUBTASK_BLOCKED",
                         {"subtask_id": subtask_id, "invocation_id": invocation_id,
                          "result_hash": result_digest, "failure_code": failure_code})

    def quarantine_effect(self, run_id: str, subtask_id: str, invocation_id: str,
                          reason: str, stop_ack: str) -> None:
        """Atomically isolate a possibly acting device and retain the intent."""
        with self._fenced_write() as c:
            row = c.execute(
                "SELECT e.resource,e.status,s.status AS step_status "
                "FROM agent_effectful_invocations e JOIN agent_subtasks s "
                "ON e.run_id=s.run_id AND e.subtask_id=s.subtask_id "
                "WHERE e.invocation_id=? AND e.run_id=? AND e.subtask_id=?",
                (invocation_id, run_id, subtask_id),
            ).fetchone()
            if row is None or row["status"] != "DISPATCHING" or row["step_status"] != "DISPATCHING":
                raise DomainError("STALE_SUBTASK_RESULT")
            c.execute(
                "INSERT OR IGNORE INTO resource_quarantine "
                "(resource,invocation_id,reason,created_at) VALUES (?,?,?,?)",
                (row["resource"], invocation_id, reason, utc_now()),
            )
            c.execute(
                "UPDATE agent_effectful_invocations SET status='QUARANTINED' "
                "WHERE invocation_id=?", (invocation_id,),
            )
            c.execute(
                "UPDATE agent_subtasks SET status='HUMAN_REVIEW',failure_code=? "
                "WHERE run_id=? AND subtask_id=?", (reason, run_id, subtask_id),
            )
            c.execute(
                "UPDATE agent_workflows SET status='HUMAN_REVIEW',failure_code=? "
                "WHERE run_id=?", (reason, run_id),
            )
            self._append(c, run_id, "EFFECTFUL_OUTCOME_QUARANTINED", {
                "subtask_id": subtask_id, "invocation_id": invocation_id,
                "resource": row["resource"], "reason": reason, "stop_ack": stop_ack,
            })

    def complete(self, run_id: str) -> None:
        with self._fenced_write() as c:
            row = self._workflow(c, run_id)
            if row["status"] == "SUCCEEDED":
                return
            if row["status"] in {"BLOCKED", "HUMAN_REVIEW"}:
                raise DomainError("WORKFLOW_TERMINAL")
            remaining = c.execute(
                "SELECT COUNT(*) FROM agent_subtasks WHERE run_id=? AND status!='SUCCEEDED'",
                (run_id,),
            ).fetchone()[0]
            if remaining:
                raise DomainError("WORKFLOW_INCOMPLETE")
            c.execute("UPDATE agent_workflows SET status='SUCCEEDED',failure_code=NULL "
                      "WHERE run_id=?", (run_id,))
            self._append(c, run_id, "WORKFLOW_COMPLETED", {})

    def block(self, run_id: str, code: str) -> None:
        with self._fenced_write() as c:
            row = self._workflow(c, run_id)
            if row["status"] in {"SUCCEEDED", "BLOCKED", "HUMAN_REVIEW"}:
                raise DomainError("WORKFLOW_TERMINAL")
            c.execute("UPDATE agent_workflows SET status='BLOCKED',failure_code=? "
                      "WHERE run_id=?", (code, run_id))
            self._append(c, run_id, "WORKFLOW_BLOCKED", {"reason": code})

    def recover_inflight(self, run_id: str) -> bool:
        """Fail closed: a persisted dispatch intent might have invoked an external tool."""
        with self._fenced_write() as c:
            row = self._workflow(c, run_id)
            if row["status"] in {"SUCCEEDED", "BLOCKED", "HUMAN_REVIEW"}:
                return False
            inflight = c.execute(
                "SELECT subtask_id FROM agent_subtasks "
                "WHERE run_id=? AND status='DISPATCHING'", (run_id,),
            ).fetchall()
            if not inflight:
                return False
            effectful = c.execute(
                "SELECT invocation_id,resource FROM agent_effectful_invocations "
                "WHERE run_id=? AND status='DISPATCHING'", (run_id,),
            ).fetchall()
            for action in effectful:
                c.execute(
                    "INSERT OR IGNORE INTO resource_quarantine "
                    "(resource,invocation_id,reason,created_at) VALUES (?,?,?,?)",
                    (action["resource"], action["invocation_id"],
                     "SUBTASK_OUTCOME_UNKNOWN", utc_now()),
                )
                c.execute(
                    "UPDATE agent_effectful_invocations SET status='QUARANTINED' "
                    "WHERE invocation_id=?", (action["invocation_id"],),
                )
            c.execute(
                "UPDATE agent_subtasks SET status='HUMAN_REVIEW',"
                "failure_code='SUBTASK_OUTCOME_UNKNOWN' "
                "WHERE run_id=? AND status='DISPATCHING'", (run_id,),
            )
            c.execute("UPDATE agent_workflows SET status='HUMAN_REVIEW',"
                      "failure_code='SUBTASK_OUTCOME_UNKNOWN' WHERE run_id=?", (run_id,))
            self._append(c, run_id, "WORKFLOW_QUARANTINED", {
                "uncertain_subtasks": [s["subtask_id"] for s in inflight],
                "quarantined_resources": [x["resource"] for x in effectful],
            })
            return True

    # v1.1.0.dev5: agent-loop feedback is persisted in the existing hashed event
    # chain. Only compact structured decisions/assessments, never raw sensor data.
    def subtask_record(self, run_id: str, subtask_id: str) -> dict[str, Any]:
        with closing(self.store._connect()) as c:
            workflow = self._workflow(c, run_id)
            self._check_chain(c, workflow)
            row = c.execute(
                "SELECT subtask_id,tool_id,success_condition,status,invocation_id,"
                "result_json,failure_code FROM agent_subtasks "
                "WHERE run_id=? AND subtask_id=?", (run_id, subtask_id),
            ).fetchone()
            if row is None:
                raise DomainError("SUBTASK_NOT_FOUND")
            record = dict(row)
            raw_result = record.pop("result_json")
            record["result"] = json.loads(raw_result) if raw_result is not None else None
            return record

    def agent_turns(self, run_id: str) -> tuple[dict[str, Any], ...]:
        with closing(self.store._connect()) as c:
            workflow = self._workflow(c, run_id)
            self._check_chain(c, workflow)
            rows = c.execute(
                "SELECT payload_json FROM agent_workflow_events "
                "WHERE run_id=? AND kind='AGENT_TURN_RECORDED' ORDER BY seq",
                (run_id,),
            ).fetchall()
            return tuple(json.loads(row["payload_json"]) for row in rows)

    def record_agent_turn(self, run_id: str, payload: dict[str, Any]) -> None:
        """Audit a semantic assessment and brain decision, distinct from dispatch."""
        bounded = _bounded_json(payload)
        step_id = bounded.get("subtask_id")
        if not isinstance(step_id, str):
            raise DomainError("INVALID_AGENT_TURN")
        with self._fenced_write() as c:
            row = self._workflow(c, run_id)
            if row["status"] == "HUMAN_REVIEW" or (
                row["status"] == "BLOCKED" and row["failure_code"] not in {
                    "READ_ONLY_TOOL_FAILED", "SUBTASK_VERIFICATION_REJECTED"
                }
            ):
                raise DomainError("WORKFLOW_TERMINAL")
            step = c.execute(
                "SELECT status,failure_code FROM agent_subtasks WHERE run_id=? AND subtask_id=?",
                (run_id, step_id),
            ).fetchone()
            if step is None or not (
                step["status"] == "SUCCEEDED" or (
                    step["status"] == "BLOCKED"
                    and step["failure_code"] in {
                        "READ_ONLY_TOOL_FAILED", "SUBTASK_VERIFICATION_REJECTED"
                    }
                )
            ):
                raise DomainError("AGENT_TURN_REQUIRES_COMPLETED_INVOCATION")
            previous = c.execute(
                "SELECT payload_json FROM agent_workflow_events "
                "WHERE run_id=? AND kind='AGENT_TURN_RECORDED' ORDER BY seq DESC",
                (run_id,),
            ).fetchall()
            attempts = sum(json.loads(e["payload_json"]).get("subtask_id") == step_id
                           for e in previous)
            if attempts >= 4:
                raise DomainError("ASSESSMENT_RETRY_BUDGET_EXHAUSTED")
            if attempts and json.loads(previous[0]["payload_json"]).get("action") != "WAIT_FOR_OBSERVATION":
                raise DomainError("AGENT_TURN_ALREADY_RECORDED")
            self._append(c, run_id, "AGENT_TURN_RECORDED", bounded)

    def replan_successor(self, run_id: str) -> str | None:
        with closing(self.store._connect()) as c:
            row = self._workflow(c, run_id)
            self._check_chain(c, row)
            links = c.execute(
                "SELECT payload_json FROM agent_workflow_events "
                "WHERE run_id=? AND kind='AGENT_REPLAN_LINKED' ORDER BY seq",
                (run_id,),
            ).fetchall()
            if len(links) > 1:
                raise DomainError("DUPLICATE_AGENT_REPLAN_LINK")
            return json.loads(links[0]["payload_json"])["next_run_id"] if links else None

    def mark_replanned(self, run_id: str, next_run_id: str) -> None:
        with self._fenced_write() as c:
            self._mark_replanned(c, run_id, next_run_id)

    def _mark_replanned(self, c: Any, run_id: str, next_run_id: str) -> None:
        if run_id == next_run_id:
            raise DomainError("AGENT_REPLAN_CYCLE")
        if not isinstance(next_run_id, str) or not 1 <= len(next_run_id) <= 128:
            raise DomainError("INVALID_WORKFLOW_ID")
        row = self._workflow(c, run_id)
        if row["status"] in {"SUCCEEDED", "HUMAN_REVIEW"} or (
            row["status"] == "BLOCKED" and row["failure_code"] not in {
                "READ_ONLY_TOOL_FAILED", "SUBTASK_VERIFICATION_REJECTED"
            }
        ):
            raise DomainError("WORKFLOW_TERMINAL")
        if c.execute(
            "SELECT 1 FROM agent_subtasks WHERE run_id=? AND status='DISPATCHING'",
            (run_id,),
        ).fetchone():
            raise DomainError("REPLAN_WHILE_DISPATCHING")
        successor = c.execute("SELECT * FROM agent_workflows WHERE run_id=?",
                              (next_run_id,)).fetchone()
        if successor is None:
            raise DomainError("REPLAN_SUCCESSOR_NOT_PERSISTED")
        self._check_chain(c, successor)
        if (successor["status"] != "PLANNED"
                or successor["catalog_hash"] != row["catalog_hash"]
                or json.loads(successor["plan_json"])["goal"] != json.loads(row["plan_json"])["goal"]
                or self._read_policy_pin(c, run_id) != self._read_policy_pin(c, next_run_id)
                or self._read_routing_pin(c, run_id) != self._read_routing_pin(c, next_run_id)):
            raise DomainError("REPLAN_SUCCESSOR_CONFIGURATION_MISMATCH")
        self._append(c, run_id, "AGENT_REPLAN_LINKED", {"next_run_id": next_run_id})
        c.execute("UPDATE agent_workflows SET status='BLOCKED',failure_code=? WHERE run_id=?",
                  ("AGENT_REPLANNED", run_id))
        self._append(c, run_id, "WORKFLOW_SUPERSEDED", {"next_run_id": next_run_id})
