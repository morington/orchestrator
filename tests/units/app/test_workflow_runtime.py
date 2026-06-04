from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.core.errors import UnsupportedVersionError
from orchestrator.core.statuses import InstanceStatus, NodeStatus

if TYPE_CHECKING:
    from .conftest import Conductor


def _payload(steps: list[dict], *, outputs: dict | None = None, idem: str = "i1", **extra) -> dict:
    base = {
        "message_version": CURRENT_MESSAGE_VERSION,
        "definition_key": "demo",
        "idempotency_key": idem,
        "steps": steps,
        **extra,
    }
    if outputs is not None:
        base["outputs"] = outputs
    return base


async def test_linear_pipeline_completes(conductor: Conductor):
    payload = _payload(
        steps=[
            {"step_id": 1, "meta": {"target": "service.a"}, "data": {}},
            {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
        ],
        outputs={"final": "$2:echo"},
    )
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.COMPLETED
    assert instance.outputs == {"final": "step-2"}
    assert len(conductor.broker.completions()) == 1


async def test_parallel_roots_dispatched_together(conductor: Conductor):
    payload = _payload(
        steps=[
            {"step_id": 1, "meta": {"target": "service.a"}, "data": {}},
            {"step_id": 2, "meta": {"target": "service.b"}, "data": {}},
        ],
    )
    await conductor.start(payload)
    await conductor.publisher.drain()

    # Оба корневых узла опубликованы в одном цикле планирования.
    assert {env["node_key"] for env in conductor.broker.invokes()} == {"step-1", "step-2"}


async def test_dependency_resolved_before_publish(make_conductor):
    captured: dict[str, dict] = {}

    def responder(env):
        captured[env["node_key"]] = env["data"]
        return {"value": 42}

    conductor = make_conductor(responder)
    payload = _payload(
        steps=[
            {"step_id": 1, "meta": {"target": "service.a"}, "data": {}},
            {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {"x": "$1:value"}},
        ],
    )
    await conductor.start(payload)
    await conductor.run_to_completion()

    assert captured["step-2"] == {"x": 42}


async def test_skip_filter_skips_node(conductor: Conductor):
    payload = _payload(
        steps=[
            {
                "step_id": 1,
                "meta": {"target": "service.a"},
                "data": {"flag": True},
                "filters": [{"filter_id": 1, "condition": "$flag == true", "then": "skip"}],
            },
        ],
    )
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    graph = await conductor.store.load_graph(resp.run_id)
    assert graph.nodes["step-1"].status == NodeStatus.SKIPPED
    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.COMPLETED


async def test_end_filter_completes_instance(conductor: Conductor):
    payload = _payload(
        steps=[
            {
                "step_id": 1,
                "meta": {"target": "service.a"},
                "data": {},
                "filters": [{"filter_id": 1, "condition": "true", "then": "end"}],
            },
            {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
        ],
    )
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.COMPLETED
    # service.b не вызывался — END завершил run.
    assert all(env["node_key"] != "step-2" for env in conductor.broker.invokes())


async def test_error_filter_fails_instance(conductor: Conductor):
    payload = _payload(
        steps=[
            {
                "step_id": 1,
                "meta": {"target": "service.a"},
                "data": {},
                "filters": [{"filter_id": 1, "condition": "true", "then": "error"}],
            },
        ],
    )
    resp = await conductor.start(payload)
    await conductor.run_to_completion()

    instance = await conductor.store.load_instance(resp.run_id)
    assert instance.status == InstanceStatus.FAILED


async def test_idempotent_start_returns_same_run(conductor: Conductor):
    payload = _payload(steps=[{"step_id": 1, "meta": {"target": "service.a"}, "data": {}}], idem="same")
    first = await conductor.start(payload)
    await conductor.run_to_completion()
    second = await conductor.start(payload)

    assert second.run_id == first.run_id
    assert second.deduplicated is True


async def test_unsupported_message_version_rejected(conductor: Conductor):
    payload = _payload(steps=[{"step_id": 1, "meta": {"target": "service.a"}, "data": {}}])
    payload["message_version"] = "9.9"
    with pytest.raises(UnsupportedVersionError):
        await conductor.start(payload)
