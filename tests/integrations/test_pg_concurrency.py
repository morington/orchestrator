from __future__ import annotations

import asyncio
from datetime import UTC

import pytest

from orchestrator.app.application.compiler import GraphCompiler
from orchestrator.app.domain.records import OutboxRecord
from orchestrator.core.entities import WorkflowDefinition

pytestmark = pytest.mark.integration


def _definition(idem: str) -> WorkflowDefinition:
    return WorkflowDefinition.model_validate(
        {
            "definition_key": "demo",
            "idempotency_key": idem,
            "steps": [{"step_id": 1, "meta": {"target": "service.a"}, "data": {}}],
        },
    )


async def test_skip_locked_outbox_claim_no_double_delivery(pg_store):
    for i in range(20):
        await pg_store.enqueue_outbox(
            OutboxRecord(run_id=f"run_{i}", subject="service.a", payload={}, delivery_id=f"dlv_{i}"),
        )

    batches = await asyncio.gather(
        pg_store.claim_outbox(20, owner="w1", lease_ttl_sec=30),
        pg_store.claim_outbox(20, owner="w2", lease_ttl_sec=30),
    )
    claimed_ids = [r.outbox_id for batch in batches for r in batch]
    # SKIP LOCKED: ни одна запись не захвачена дважды.
    assert len(claimed_ids) == len(set(claimed_ids))
    assert len(claimed_ids) == 20


async def test_concurrent_create_instance_idempotent(pg_store):
    async def _create() -> tuple[str, bool]:
        instance, graph = GraphCompiler().compile(_definition("race-key"))
        saved, _, created = await pg_store.create_instance(instance, graph)
        return saved.run_id, created

    results = await asyncio.gather(*[_create() for _ in range(8)])
    run_ids = {run_id for run_id, _ in results}
    created_flags = [created for _, created in results]

    # Гонка стартов с одним idempotency_key → ровно один run и ровно один created=True.
    assert len(run_ids) == 1
    assert created_flags.count(True) == 1


async def test_inbox_unique_under_race(pg_store):
    results = await asyncio.gather(*[pg_store.register_inbox("op_x", 1, message_id="m") for _ in range(10)])
    assert results.count("applied") == 1
    assert results.count("duplicate") == 9


async def test_reclaim_expired_leases(pg_store):
    from datetime import datetime, timedelta

    instance, graph = GraphCompiler().compile(_definition("lease-key"))
    await pg_store.create_instance(instance, graph)
    node = graph.nodes["step-1"]
    node.locked_by = "dead-replica"
    node.locked_until = datetime.now(tz=UTC) - timedelta(seconds=60)
    await pg_store.persist_node(instance.run_id, node)

    reclaimed = await pg_store.reclaim_expired_leases(datetime.now(tz=UTC))
    assert reclaimed >= 1
    reloaded = await pg_store.load_graph(instance.run_id)
    assert reloaded.nodes["step-1"].locked_by is None
