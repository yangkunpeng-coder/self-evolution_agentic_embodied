"""Deterministic Fake. This is not a robot controller or certified safety adapter."""

from time import sleep

from ...core.canonical_json import content_hash
from ...core.contracts import AuthorizedInvocation, DomainError, Observation, ProviderResponse
from ...core.ids import uid, utc_now


class FakeGateway:
    def __init__(self, scenario: str = "success") -> None:
        if scenario not in {"success", "goal_fail", "unauthorized", "missing_after", "unknown_stop",
                            "slow_invoke", "slow_stop", "rejected_stop"}:
            raise ValueError("invalid Fake scenario")
        self.scenario = scenario
        self.position = "start"
        self.goal_reached = False
        self.invocations: list[str] = []
        self.stop_requests: list[str] = []

    def observe(self, task_id: str, release_id: str, invocation_id: str | None) -> Observation | None:
        if self.scenario == "missing_after" and invocation_id is not None:
            return None
        return Observation(uid(), task_id, release_id, invocation_id, utc_now(),
                           {"position": self.position, "goal_reached": self.goal_reached})

    def invoke(self, ticket: AuthorizedInvocation, arguments: dict) -> ProviderResponse:
        if ticket.release_id != "R0-FAKE" or ticket.tool_id != "fake.move" or content_hash(arguments) != ticket.arguments_hash:
            raise DomainError("INVALID_AUTHORIZED_INVOCATION")
        self.invocations.append(ticket.invocation_id)
        if self.scenario in {"slow_invoke", "slow_stop", "rejected_stop"}:
            sleep(0.25)
        if self.scenario == "unknown_stop":
            raise TimeoutError("Fake invocation exceeded deadline; side effect unknown")
        self.position = arguments["target"]
        self.goal_reached = self.scenario != "goal_fail"
        return ProviderResponse(ticket.invocation_id, "COMPLETED", "YES")

    def request_stop(self, invocation_id: str) -> str:
        self.stop_requests.append(invocation_id)
        if self.scenario == "slow_stop":
            sleep(0.25)
        if self.scenario == "rejected_stop":
            return "REJECTED"
        return "UNKNOWN" if self.scenario == "unknown_stop" else "CONFIRMED"
