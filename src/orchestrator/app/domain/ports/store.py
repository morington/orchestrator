from datetime import datetime
from typing import Protocol

from orchestrator.app.domain.graph import ExecutionGraph, ExecutionNode
from orchestrator.app.domain.instance import WorkflowInstance
from orchestrator.app.domain.messages import InboxOutcome
from orchestrator.app.domain.records import DeadLetterRecord, OutboxRecord


class WorkflowStore(Protocol):
    """
    Порт персистентности run state (instance + граф + надёжность).

    Реализации: InMemoryWorkflowStore (тесты), SqlAlchemyWorkflowStore (PG/SQLite).
    Транзакционные гарантии (атомарность node+outbox, SKIP LOCKED) — зона реализации.
    """

    async def create_instance(
        self, instance: WorkflowInstance, graph: ExecutionGraph,
    ) -> tuple[WorkflowInstance, ExecutionGraph, bool]:
        """
        Идемпотентно создать run по idempotency_key.

        Returns:
            (instance, graph, created): при повторном idempotency_key created=False и
            возвращаются существующие instance + граф.
        """
        ...

    async def load_instance(self, run_id: str) -> WorkflowInstance | None:
        """Загрузить instance по run_id."""
        ...

    async def load_graph(self, run_id: str) -> ExecutionGraph:
        """Загрузить замороженный граф run."""
        ...

    async def persist_instance(self, instance: WorkflowInstance) -> None:
        """Сохранить instance (optimistic locking по revision)."""
        ...

    async def persist_node(self, run_id: str, node: ExecutionNode) -> None:
        """Сохранить узел (optimistic locking по revision)."""
        ...

    async def enqueue_dispatch(self, run_id: str, node: ExecutionNode, outbox: OutboxRecord) -> None:
        """Атомарно сохранить узел (ENQUEUED) и outbox-запись (pending)."""
        ...

    async def enqueue_outbox(self, outbox: OutboxRecord) -> None:
        """Поставить outbox-запись (например, workflow_completed)."""
        ...

    async def claim_outbox(self, limit: int, owner: str, lease_ttl_sec: int) -> list[OutboxRecord]:
        """Захватить pending outbox-записи с lease (SKIP LOCKED в PG)."""
        ...

    async def complete_outbox(self, outbox_id: int, *, success: bool) -> None:
        """Отметить outbox sent/failed."""
        ...

    async def register_inbox(self, step_run_id: str, attempt: int, message_id: str | None) -> InboxOutcome:
        """Зарегистрировать входящий результат; UNIQUE(step_run_id, attempt)."""
        ...

    async def dead_letter(self, record: DeadLetterRecord) -> None:
        """Записать сообщение в dead-letter."""
        ...

    async def claim_runnable_instances(self, limit: int, owner: str, lease_ttl_sec: int) -> list[str]:
        """Захватить активные run_id для пересчёта расписания (recovery / scheduler)."""
        ...

    async def find_expired_nodes(self, now: datetime, limit: int) -> list[tuple[str, str]]:
        """Найти узлы с истёкшим deadline_at в (run_id, node_key)."""
        ...

    async def reclaim_expired_leases(self, now: datetime) -> int:
        """Снять истёкшие leases; вернуть число затронутых строк."""
        ...
