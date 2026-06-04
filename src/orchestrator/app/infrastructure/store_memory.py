import asyncio
from datetime import datetime

from orchestrator.app.domain.graph import ExecutionGraph, ExecutionNode
from orchestrator.app.domain.instance import WorkflowInstance
from orchestrator.app.domain.messages import InboxOutcome
from orchestrator.app.domain.records import DeadLetterRecord, OutboxRecord
from orchestrator.core.statuses import NodeStatus, is_instance_terminal


class InMemoryWorkflowStore:
    """
    In-memory реализация WorkflowStore для unit-тестов и dev playground.

    Хранит живые объекты графа (без копий): load_graph возвращает тот же объект, что
    создаётся при старте. Не переживает рестарт и не доказывает concurrency — это зона
    SqlAlchemyWorkflowStore + PG integration tests.
    """

    def __init__(self) -> None:
        self._instances: dict[str, WorkflowInstance] = {}
        self._graphs: dict[str, ExecutionGraph] = {}
        self._idempotency: dict[str, str] = {}
        self._outbox: list[OutboxRecord] = []
        self._inbox: set[tuple[str, int]] = set()
        self.dead_letters: list[DeadLetterRecord] = []
        self._next_instance_id = 1
        self._next_outbox_id = 1
        self._lock = asyncio.Lock()

    async def create_instance(
        self, instance: WorkflowInstance, graph: ExecutionGraph,
    ) -> tuple[WorkflowInstance, ExecutionGraph, bool]:
        async with self._lock:
            existing_run = self._idempotency.get(instance.idempotency_key)
            if existing_run is not None:
                return self._instances[existing_run], self._graphs[existing_run], False

            instance.instance_id = self._next_instance_id
            instance.created_at = datetime.now(tz=None)
            self._next_instance_id += 1
            self._instances[instance.run_id] = instance
            self._graphs[instance.run_id] = graph
            self._idempotency[instance.idempotency_key] = instance.run_id
            return instance, graph, True

    async def load_instance(self, run_id: str) -> WorkflowInstance | None:
        return self._instances.get(run_id)

    async def load_graph(self, run_id: str) -> ExecutionGraph:
        return self._graphs[run_id]

    async def persist_instance(self, instance: WorkflowInstance) -> None:
        instance.revision += 1
        self._instances[instance.run_id] = instance

    async def persist_node(self, run_id: str, node: ExecutionNode) -> None:
        node.revision += 1

    async def enqueue_dispatch(self, run_id: str, node: ExecutionNode, outbox: OutboxRecord) -> None:
        node.revision += 1
        await self.enqueue_outbox(outbox)

    async def enqueue_outbox(self, outbox: OutboxRecord) -> None:
        async with self._lock:
            outbox.outbox_id = self._next_outbox_id
            outbox.created_at = datetime.now(tz=None)
            self._next_outbox_id += 1
            self._outbox.append(outbox)

    async def claim_outbox(self, limit: int, owner: str, lease_ttl_sec: int) -> list[OutboxRecord]:
        async with self._lock:
            pending = [o for o in self._outbox if o.status == "pending"][:limit]
            for record in pending:
                record.status = "claimed"
                record.locked_by = owner
            return pending

    async def complete_outbox(self, outbox_id: int, *, success: bool) -> None:
        async with self._lock:
            for record in self._outbox:
                if record.outbox_id == outbox_id:
                    record.status = "sent" if success else "failed"
                    if success:
                        record.sent_at = datetime.now(tz=None)
                    return

    async def register_inbox(self, step_run_id: str, attempt: int, message_id: str | None) -> InboxOutcome:
        async with self._lock:
            key = (step_run_id, attempt)
            if key in self._inbox:
                return "duplicate"
            self._inbox.add(key)
            return "applied"

    async def dead_letter(self, record: DeadLetterRecord) -> None:
        record.created_at = datetime.now(tz=None)
        self.dead_letters.append(record)

    async def claim_runnable_instances(self, limit: int, owner: str, lease_ttl_sec: int) -> list[str]:
        return [
            run_id
            for run_id, instance in self._instances.items()
            if not is_instance_terminal(instance.status)
        ][:limit]

    async def find_expired_nodes(self, now: datetime, limit: int) -> list[tuple[str, str]]:
        watched = (NodeStatus.WAITING_RESULT, NodeStatus.ENQUEUED, NodeStatus.DISPATCHED)
        expired = [
            (run_id, node.node_key)
            for run_id, graph in self._graphs.items()
            for node in graph.nodes.values()
            if node.status in watched and node.deadline_at is not None and node.deadline_at < now
        ]
        return expired[:limit]

    async def reclaim_expired_leases(self, now: datetime) -> int:
        return 0

    # --- доступ для тестов / dev ------------------------------------------

    def pending_outbox(self) -> list[OutboxRecord]:
        return [o for o in self._outbox if o.status in ("pending", "claimed")]
