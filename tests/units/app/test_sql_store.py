from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from orchestrator.app.application.compiler import GraphCompiler
from orchestrator.app.domain.records import DeadLetterRecord, OutboxRecord
from orchestrator.app.infrastructure.db.base import Base
from orchestrator.app.infrastructure.store_sql import SqlAlchemyWorkflowStore
from orchestrator.core.entities import WorkflowDefinition
from orchestrator.core.statuses import NodeStatus


@pytest.fixture
async def store() -> SqlAlchemyWorkflowStore:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield SqlAlchemyWorkflowStore(factory, dialect="sqlite")
    await engine.dispose()


def _definition(idem: str = "i") -> WorkflowDefinition:
    return WorkflowDefinition.model_validate(
        {
            "definition_key": "demo",
            "idempotency_key": idem,
            "outputs": {"final": "$1:echo"},
            "steps": [
                {"step_id": 1, "meta": {"target": "service.a"}, "data": {}},
                {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
            ],
        },
    )


async def test_create_instance_persists_graph(store: SqlAlchemyWorkflowStore):
    instance, graph = GraphCompiler().compile(_definition())
    saved, _, created = await store.create_instance(instance, graph)
    assert created is True
    assert saved.instance_id is not None

    loaded = await store.load_graph(saved.run_id)
    assert set(loaded.nodes) == {"step-1", "step-2"}
    assert loaded.nodes["step-2"].depends_on[0].node == 1


async def test_create_instance_idempotent(store: SqlAlchemyWorkflowStore):
    instance, graph = GraphCompiler().compile(_definition("same"))
    first, _, c1 = await store.create_instance(instance, graph)
    second_instance, second_graph = GraphCompiler().compile(_definition("same"))
    second, _, c2 = await store.create_instance(second_instance, second_graph)

    assert c1 is True
    assert c2 is False
    assert second.run_id == first.run_id


async def test_persist_node_round_trip(store: SqlAlchemyWorkflowStore):
    instance, graph = GraphCompiler().compile(_definition("n1"))
    await store.create_instance(instance, graph)

    node = graph.nodes["step-1"]
    node.status = NodeStatus.COMPLETED
    node.result = {"echo": "ok"}
    await store.persist_node(instance.run_id, node)

    reloaded = await store.load_graph(instance.run_id)
    assert reloaded.nodes["step-1"].status == NodeStatus.COMPLETED
    assert reloaded.nodes["step-1"].result == {"echo": "ok"}


async def test_inbox_dedup(store: SqlAlchemyWorkflowStore):
    assert await store.register_inbox("op_1", 1, message_id="m1") == "applied"
    assert await store.register_inbox("op_1", 1, message_id="m1") == "duplicate"
    assert await store.register_inbox("op_1", 2, message_id="m2") == "applied"


async def test_outbox_claim_and_complete(store: SqlAlchemyWorkflowStore):
    await store.enqueue_outbox(
        OutboxRecord(run_id="run_1", subject="service.a", payload={}, delivery_id="dlv_1", kind="invoke"),
    )
    claimed = await store.claim_outbox(10, owner="w1", lease_ttl_sec=30)
    assert len(claimed) == 1
    await store.complete_outbox(claimed[0].outbox_id, success=True)

    assert await store.claim_outbox(10, owner="w1", lease_ttl_sec=30) == []


async def test_dead_letter_recorded(store: SqlAlchemyWorkflowStore):
    await store.dead_letter(DeadLetterRecord(subject="orchestrator.results", reason="late_result", payload={}))
    # повторный claim не падает; запись просто сохранена
    assert await store.claim_runnable_instances(10, "w1", 30) == []


async def test_runtime_linear_pipeline_over_sqlite(store: SqlAlchemyWorkflowStore):
    from orchestrator.app.application.runtime import WorkflowRuntime
    from orchestrator.app.application.workers import OutboxPublisher
    from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION

    published: list[tuple[str, dict]] = []

    class _Broker:
        async def publish(self, subject, payload, *, msg_id=None):
            published.append((subject, payload))
            return True

    runtime = WorkflowRuntime(store, completed_subject="orchestrator.workflow.completed")
    publisher = OutboxPublisher(store, _Broker(), runtime, batch=100)

    resp = await runtime.start(
        {
            "message_version": CURRENT_MESSAGE_VERSION,
            "definition_key": "demo",
            "idempotency_key": "sql-e2e",
            "outputs": {"final": "$2:echo"},
            "steps": [
                {"step_id": 1, "meta": {"target": "service.a"}, "data": {}},
                {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
            ],
        },
    )

    answered: set[tuple[str, int]] = set()
    for _ in range(20):
        await publisher.drain()
        pending = [
            p for s, p in published if p.get("node_key") and (p["step_run_id"], p["attempt"]) not in answered
        ]
        if not pending:
            break
        for env in pending:
            answered.add((env["step_run_id"], env["attempt"]))
            await runtime.on_result(
                {
                    "message_version": CURRENT_MESSAGE_VERSION,
                    "definition_key": env["definition_key"],
                    "run_id": env["run_id"],
                    "node_key": env["node_key"],
                    "step_run_id": env["step_run_id"],
                    "attempt": env["attempt"],
                    "result": {"echo": env["node_key"]},
                },
            )

    instance = await store.load_instance(resp.run_id)
    assert instance.status.value == "completed"
    assert instance.outputs == {"final": "step-2"}
    assert any(s == "orchestrator.workflow.completed" for s, _ in published)
