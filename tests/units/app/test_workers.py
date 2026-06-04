from __future__ import annotations

from datetime import UTC, datetime, timedelta

from orchestrator.app.application.workers import TimeoutWatcher
from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.core.statuses import InstanceStatus, NodeStatus


async def test_timeout_watcher_expires_waiting_node(conductor):
    payload = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": "timeout-1",
        "steps": [{"step_id": 1, "meta": {"target": "service.a", "on_failure": "fail"}, "data": {}}],
    }
    resp = await conductor.start(payload)
    await conductor.publisher.drain()

    graph = await conductor.store.load_graph(resp.run_id)
    node = graph.nodes["step-1"]
    assert node.status == NodeStatus.WAITING_RESULT
    node.deadline_at = datetime.now(tz=UTC) - timedelta(seconds=1)

    watcher = TimeoutWatcher(conductor.store, conductor.runtime)
    processed = await watcher.run_once()

    assert processed == 1
    assert graph.nodes["step-1"].status == NodeStatus.EXPIRED
    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.FAILED


async def test_outbox_publishes_completion_once(conductor):
    payload = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": "once-1",
        "steps": [{"step_id": 1, "meta": {"target": "service.a"}, "data": {}}],
    }
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    # повторный прогон publisher не должен публиковать второй completion
    await conductor.publisher.drain()
    assert len(conductor.broker.completions()) == 1

    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.completion_published_at is not None
