from __future__ import annotations

from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.core.statuses import InstanceStatus, NodeStatus


def _error_result(env: dict, failure_class: str = "business") -> dict:
    return {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": env["definition_key"],
        "run_id": env["run_id"],
        "node_key": env["node_key"],
        "step_run_id": env["step_run_id"],
        "attempt": env["attempt"],
        "error": {"failure_class": failure_class, "message": "boom"},
    }


def _payload(on_failure: str, idem: str) -> dict:
    return {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": idem,
        "steps": [
            {"step_id": 1, "meta": {"target": "service.a", "on_failure": on_failure}, "data": {}},
            {
                "step_id": 2,
                "meta": {"target": "service.b"},
                "depends_on": [{"node": 1, "policy": "requires_closed"}],
                "data": {},
            },
        ],
    }


async def test_on_failure_fail_fails_instance(conductor):
    resp = await conductor.start(_payload("fail", "fail-1"))
    await conductor.publisher.drain()
    env = conductor.broker.invokes()[0]
    await conductor.runtime.on_result(_error_result(env))

    graph = await conductor.store.load_graph(resp.run_id)
    assert graph.nodes["step-1"].status == NodeStatus.FAILED
    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.FAILED


async def test_on_failure_skip_continues(conductor):
    resp = await conductor.start(_payload("skip", "skip-1"))
    await conductor.publisher.drain()
    env = conductor.broker.invokes()[0]
    await conductor.runtime.on_result(_error_result(env))
    await conductor.run_to_completion()

    graph = await conductor.store.load_graph(resp.run_id)
    assert graph.nodes["step-1"].status == NodeStatus.SKIPPED
    assert graph.nodes["step-2"].status == NodeStatus.COMPLETED
    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.COMPLETED


async def test_on_failure_abandon_continues(conductor):
    resp = await conductor.start(_payload("abandon", "abandon-1"))
    await conductor.publisher.drain()
    env = conductor.broker.invokes()[0]
    await conductor.runtime.on_result(_error_result(env))
    await conductor.run_to_completion()

    graph = await conductor.store.load_graph(resp.run_id)
    assert graph.nodes["step-1"].status == NodeStatus.ABANDONED
    assert graph.nodes["step-2"].status == NodeStatus.COMPLETED
