from __future__ import annotations

from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.core.statuses import NodeStatus


def _single_step_payload(idem: str) -> dict:
    return {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": idem,
        "steps": [{"step_id": 1, "meta": {"target": "service.a"}, "data": {}}],
    }


async def _start_and_get_invoke(conductor, idem: str) -> dict:
    await conductor.start(_single_step_payload(idem))
    await conductor.publisher.drain()
    return conductor.broker.invokes()[0]


async def test_duplicate_result_is_ignored(conductor):
    env = await _start_and_get_invoke(conductor, "dup-1")
    result = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": env["definition_key"],
        "run_id": env["run_id"],
        "node_key": env["node_key"],
        "step_run_id": env["step_run_id"],
        "attempt": env["attempt"],
        "result": {"v": 1},
    }
    await conductor.runtime.on_result(result)
    await conductor.runtime.on_result(result)  # duplicate

    graph = await conductor.store.load_graph(env["run_id"])
    assert graph.nodes["step-1"].status == NodeStatus.COMPLETED
    assert conductor.store.dead_letters == []


async def test_late_result_dead_lettered(conductor):
    env = await _start_and_get_invoke(conductor, "late-1")
    accepted = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": env["definition_key"],
        "run_id": env["run_id"],
        "node_key": env["node_key"],
        "step_run_id": env["step_run_id"],
        "attempt": env["attempt"],
        "result": {"v": 1},
    }
    await conductor.runtime.on_result(accepted)

    late = dict(accepted)
    late["step_run_id"] = "op_other"
    late["attempt"] = 2
    await conductor.runtime.on_result(late)

    assert any(dl.reason == "late_result" for dl in conductor.store.dead_letters)


async def test_result_before_dispatch_ack_is_accepted(conductor):
    # Узел ещё ENQUEUED (mark_dispatched не вызван), а результат уже пришёл.
    response = await conductor.start(_single_step_payload("early-1"))
    run_id = response.run_id
    graph = await conductor.store.load_graph(run_id)
    node = graph.nodes["step-1"]
    assert node.status == NodeStatus.ENQUEUED

    await conductor.runtime.on_result(
        {
            "message_version": CURRENT_MESSAGE_VERSION,
            "definition_key": "demo",
            "run_id": run_id,
            "node_key": "step-1",
            "step_run_id": node.step_run_id,
            "attempt": node.attempt,
            "result": {"v": 1},
        },
    )

    assert graph.nodes["step-1"].status == NodeStatus.COMPLETED
    assert conductor.store.dead_letters == []


async def test_unsupported_version_result_dead_lettered(conductor):
    env = await _start_and_get_invoke(conductor, "ver-1")
    await conductor.runtime.on_result(
        {
            "message_version": "9.9",
            "definition_key": env["definition_key"],
            "run_id": env["run_id"],
            "node_key": env["node_key"],
            "step_run_id": env["step_run_id"],
            "attempt": env["attempt"],
            "result": {},
        },
    )
    assert any(dl.reason == "unsupported_message_version" for dl in conductor.store.dead_letters)
