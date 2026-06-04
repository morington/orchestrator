from collections.abc import Callable
from typing import Any

import pytest

from orchestrator.app.application.runtime import WorkflowRuntime
from orchestrator.app.application.workers import OutboxPublisher
from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.app.infrastructure.store_memory import InMemoryWorkflowStore

COMPLETED_SUBJECT = "orchestrator.workflow.completed"


class FakeBroker:
    """In-memory publisher: пишет сообщения и умеет имитировать сбои публикации."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.fail_counts: dict[str, int] = {}

    def fail_next(self, subject: str, times: int) -> None:
        self.fail_counts[subject] = times

    async def publish(self, subject: str, payload: dict[str, Any], *, msg_id: str | None = None) -> bool:
        remaining = self.fail_counts.get(subject, 0)
        if remaining > 0:
            self.fail_counts[subject] = remaining - 1
            return False
        self.published.append((subject, payload))
        return True

    def invokes(self) -> list[dict[str, Any]]:
        return [p for s, p in self.published if p.get("node_key") and s != COMPLETED_SUBJECT]

    def completions(self) -> list[dict[str, Any]]:
        return [p for s, p in self.published if s == COMPLETED_SUBJECT]


class Conductor:
    """
    Harness: связывает store + broker + runtime + outbox publisher и автоматически
    отвечает на исходящие invoke результатами от `responder`, доводя run до конца.
    """

    def __init__(self, responder: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None) -> None:
        self.store = InMemoryWorkflowStore()
        self.broker = FakeBroker()
        self.runtime = WorkflowRuntime(self.store, completed_subject=COMPLETED_SUBJECT)
        self.publisher = OutboxPublisher(self.store, self.broker, self.runtime, batch=100)
        self.responder = responder or (lambda env: {"echo": env["node_key"]})
        self._answered: set[tuple[str, int]] = set()

    async def start(self, payload: dict[str, Any]):
        return await self.runtime.start(payload)

    async def run_to_completion(self, max_rounds: int = 50) -> None:
        for _ in range(max_rounds):
            await self.publisher.drain()
            new_invokes = [
                env
                for env in self.broker.invokes()
                if (env["step_run_id"], env["attempt"]) not in self._answered
                and env.get("transport_mode") != "fire_and_forget"
            ]
            if not new_invokes:
                return
            for env in new_invokes:
                self._answered.add((env["step_run_id"], env["attempt"]))
                result = self.responder(env)
                if result is not None:
                    await self.runtime.on_result(self._result_message(env, result=result))

    def _result_message(self, env: dict[str, Any], *, result: dict[str, Any]) -> dict[str, Any]:
        return {
            "message_version": CURRENT_MESSAGE_VERSION,
            "definition_key": env["definition_key"],
            "run_id": env["run_id"],
            "node_key": env["node_key"],
            "step_run_id": env["step_run_id"],
            "attempt": env["attempt"],
            "result": result,
        }


@pytest.fixture
def conductor() -> Conductor:
    return Conductor()


@pytest.fixture
def make_conductor() -> Callable[..., Conductor]:
    def _factory(responder: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None) -> Conductor:
        return Conductor(responder=responder)

    return _factory
