from __future__ import annotations

from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.core.statuses import InstanceStatus, NodeStatus


async def test_goto_marks_bypassed_skipped_by_goto(conductor):
    payload = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": "goto-1",
        "steps": [
            {
                "step_id": 1,
                "meta": {"target": "service.a"},
                "data": {},
                "filters": [{"filter_id": 1, "condition": "true", "then": "goto", "targets": [4]}],
            },
            {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
            {"step_id": 3, "meta": {"target": "service.c"}, "depends_on": [2], "data": {}},
            {"step_id": 4, "meta": {"target": "service.d"}, "depends_on": [3], "data": {}},
        ],
    }
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    graph = await conductor.store.load_graph(resp.run_id)
    assert graph.nodes["step-1"].status == NodeStatus.SKIPPED
    assert graph.nodes["step-2"].status == NodeStatus.SKIPPED_BY_GOTO
    assert graph.nodes["step-3"].status == NodeStatus.SKIPPED_BY_GOTO
    assert graph.nodes["step-4"].status == NodeStatus.COMPLETED

    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.COMPLETED
    assert {env["node_key"] for env in conductor.broker.invokes()} == {"step-4"}
