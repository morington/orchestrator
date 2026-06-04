from typing import Any

import pytest

from orchestrator.app.application.admin import AdminService
from orchestrator.app.application.metrics import Metrics
from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.app.presentation.handlers import NatsHandlers
from orchestrator.core.statuses import InstanceStatus


def _definition(idem: str, target: str = "service.a") -> dict[str, Any]:
    return {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": idem,
        "steps": [{"step_id": 1, "meta": {"target": target}, "data": {}}],
    }


class Harness:
    """Связка NatsHandlers + AdminService поверх Conductor (in-memory store/broker)."""

    def __init__(self, conductor) -> None:
        self.conductor = conductor
        self.runtime = conductor.runtime
        self.store = conductor.store
        self.broker = conductor.broker
        self.publisher = conductor.publisher
        self.handlers = NatsHandlers(self.runtime, AdminService(self.runtime), Metrics())


@pytest.fixture
def harness(make_conductor) -> Harness:
    return Harness(make_conductor())


async def test_handle_start_creates_and_returns_run(harness: Harness) -> None:
    response = await harness.handlers.handle_start(_definition("k1"))
    assert response["deduplicated"] is False
    assert response["run_id"]
    assert response["instance_status"] == InstanceStatus.RUNNING.value


async def test_handle_start_is_idempotent(harness: Harness) -> None:
    first = await harness.handlers.handle_start(_definition("same"))
    second = await harness.handlers.handle_start(_definition("same"))
    assert second["deduplicated"] is True
    assert first["run_id"] == second["run_id"]


async def test_handle_start_rejects_unsupported_version(harness: Harness) -> None:
    payload = _definition("kv")
    payload["message_version"] = "99.0"
    response = await harness.handlers.handle_start(payload)
    assert response["error"] == "unsupported_version"


async def test_handle_result_completes_run(harness: Harness) -> None:
    response = await harness.handlers.handle_start(_definition("kr"))
    run_id = response["run_id"]
    await harness.publisher.drain()
    env = harness.broker.invokes()[0]
    await harness.handlers.handle_result(
        {
            "message_version": CURRENT_MESSAGE_VERSION,
            "definition_key": env["definition_key"],
            "run_id": run_id,
            "node_key": env["node_key"],
            "step_run_id": env["step_run_id"],
            "attempt": env["attempt"],
            "result": {"ok": True},
        },
    )
    instance = await harness.store.load_instance(run_id)
    assert instance.status == InstanceStatus.COMPLETED


async def test_handle_cancel_marks_run_cancelled(harness: Harness) -> None:
    run_id = (await harness.handlers.handle_start(_definition("kc")))["run_id"]
    response = await harness.handlers.handle_cancel({"run_id": run_id})
    assert response["cancelled"] is True
    instance = await harness.store.load_instance(run_id)
    assert instance.status == InstanceStatus.CANCELLED


async def test_handle_cancel_noop_for_unknown_run(harness: Harness) -> None:
    response = await harness.handlers.handle_cancel({"run_id": "missing"})
    assert response["cancelled"] is False


async def test_admin_inspect_lists_nodes(harness: Harness) -> None:
    run_id = (await harness.handlers.handle_start(_definition("ki")))["run_id"]
    report = await harness.handlers.handle_admin("inspect", {"run_id": run_id})
    assert report["run_id"] == run_id
    assert len(report["nodes"]) == 1


async def test_admin_retry_node_requires_terminal_status(harness: Harness) -> None:
    run_id = (await harness.handlers.handle_start(_definition("kt")))["run_id"]
    graph = await harness.store.load_graph(run_id)
    node_key = next(iter(graph.nodes))
    report = await harness.handlers.handle_admin("retry_node", {"run_id": run_id, "node_key": node_key})
    assert report["error"] == "admin_error"


async def test_admin_retry_node_resets_failed_leaf(harness: Harness) -> None:
    from orchestrator.core.statuses import NodeStatus

    run_id = (await harness.handlers.handle_start(_definition("kf")))["run_id"]
    graph = await harness.store.load_graph(run_id)
    node = next(iter(graph.nodes.values()))
    node.status = NodeStatus.FAILED
    await harness.store.persist_node(run_id, node)

    report = await harness.handlers.handle_admin("retry_node", {"run_id": run_id, "node_key": node.node_key})
    assert "error" not in report
    refreshed = (await harness.store.load_graph(run_id)).nodes[node.node_key]
    assert refreshed.status != NodeStatus.FAILED
    assert refreshed.attempt == 0 or refreshed.status == NodeStatus.ENQUEUED


async def test_admin_unknown_command(harness: Harness) -> None:
    run_id = (await harness.handlers.handle_start(_definition("ku")))["run_id"]
    report = await harness.handlers.handle_admin("explode", {"run_id": run_id})
    assert report["error"] == "unknown_command"
