from __future__ import annotations

from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.core.statuses import InstanceStatus, NodeStatus


async def test_dispatch_retry_then_success(conductor):
    conductor.broker.fail_next("service.a", times=1)
    payload = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": "retry-1",
        "outputs": {"final": "$1:echo"},
        "steps": [
            {
                "step_id": 1,
                "meta": {"target": "service.a", "retry_policy": {"max_attempts": 3}},
                "data": {},
            },
        ],
    }
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    graph = await conductor.store.load_graph(resp.run_id)
    node = graph.nodes["step-1"]
    assert node.status == NodeStatus.COMPLETED
    assert node.attempt == 2  # первая попытка провалилась на publish

    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.COMPLETED


async def test_dispatch_failed_when_attempts_exhausted(conductor):
    conductor.broker.fail_next("service.a", times=5)
    payload = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": "retry-2",
        "steps": [
            {
                "step_id": 1,
                "meta": {"target": "service.a", "retry_policy": {"max_attempts": 2}, "on_failure": "fail"},
                "data": {},
            },
        ],
    }
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    graph = await conductor.store.load_graph(resp.run_id)
    assert graph.nodes["step-1"].status == NodeStatus.DISPATCH_FAILED
    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.FAILED
