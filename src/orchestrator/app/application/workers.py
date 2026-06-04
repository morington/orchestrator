import asyncio
import contextlib
from datetime import UTC, datetime

import structlog

from orchestrator.app.application.runtime import WorkflowRuntime
from orchestrator.app.configuration.loggers import Loggers
from orchestrator.app.domain.ports.broker import MessagePublisher
from orchestrator.app.domain.ports.store import WorkflowStore

logger = structlog.getLogger(Loggers.engine.name)


def _now() -> datetime:
    return datetime.now(tz=UTC)


class OutboxPublisher:
    """
    Публикует pending outbox-записи в NATS (outbox pattern).

    invoke → runtime.mark_dispatched(ack); workflow_completed → пометить
    completion_published_at идемпотентно. Запускается периодически и при recovery.
    """

    def __init__(
        self,
        store: WorkflowStore,
        broker: MessagePublisher,
        runtime: WorkflowRuntime,
        *,
        owner: str = "orchestrator-1",
        batch: int = 50,
        lease_ttl_sec: int = 30,
        poll_sec: float = 1.0,
    ) -> None:
        self.store = store
        self.broker = broker
        self.runtime = runtime
        self.owner = owner
        self.batch = batch
        self.lease_ttl_sec = lease_ttl_sec
        self.poll_sec = poll_sec
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        """Опубликовать один батч outbox; вернуть число обработанных записей."""
        records = await self.store.claim_outbox(self.batch, self.owner, self.lease_ttl_sec)
        for record in records:
            ack = await self.broker.publish(record.subject, record.payload, msg_id=record.delivery_id)
            await self.store.complete_outbox(record.outbox_id, success=ack)

            if record.kind == "invoke" and record.node_key is not None:
                await self.runtime.mark_dispatched(record.run_id, record.node_key, ack=ack)
            elif record.kind == "workflow_completed" and ack:
                instance = await self.store.load_instance(record.run_id)
                if instance is not None and instance.completion_published_at is None:
                    instance.completion_published_at = _now()
                    await self.store.persist_instance(instance)
        return len(records)

    async def drain(self, max_passes: int = 100) -> None:
        """Опубликовать всё, пока есть pending (для тестов и shutdown)."""
        for _ in range(max_passes):
            if await self.run_once() == 0:
                return

    async def start(self) -> None:
        self._stopped.clear()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except Exception:
                await logger.aexception("outbox publish loop error")
            await asyncio.sleep(self.poll_sec)


class TimeoutWatcher:
    """
    Периодически переводит просроченные узлы (deadline_at < now) в EXPIRED и применяет
    on_failure, не завися от живого NATS/HTTP-запроса (docs/SEMANTICS §3).
    """

    def __init__(
        self,
        store: WorkflowStore,
        runtime: WorkflowRuntime,
        *,
        batch: int = 100,
        poll_sec: float = 5.0,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.batch = batch
        self.poll_sec = poll_sec
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        """Обработать просроченные узлы; вернуть их число."""
        expired = await self.store.find_expired_nodes(_now(), self.batch)
        for run_id, node_key in expired:
            await self.runtime.expire_node(run_id, node_key)
        return len(expired)

    async def start(self) -> None:
        self._stopped.clear()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except Exception:
                await logger.aexception("timeout watcher loop error")
            await asyncio.sleep(self.poll_sec)


class RecoveryOnStartup:
    """
    Восстановление при старте процесса: снять истёкшие leases, дослать pending outbox,
    обработать просроченные узлы. Дубли publish исключены идемпотентностью outbox.
    """

    def __init__(
        self,
        store: WorkflowStore,
        outbox_publisher: OutboxPublisher,
        timeout_watcher: TimeoutWatcher,
    ) -> None:
        self.store = store
        self.outbox_publisher = outbox_publisher
        self.timeout_watcher = timeout_watcher

    async def run(self) -> None:
        reclaimed = await self.store.reclaim_expired_leases(_now())
        await self.outbox_publisher.run_once()
        await self.timeout_watcher.run_once()
        await logger.ainfo("recovery on startup complete", reclaimed_leases=reclaimed)


class RetentionCleanupWorker:
    """
    Периодическая очистка terminal-данных по политике retention (см. docs/OPERATIONS.md).

    Инварианты: не удалять non-terminal instances и rows с активным lease.
    """

    def __init__(self, store: WorkflowStore, *, poll_sec: float = 3600.0) -> None:
        self.store = store
        self.poll_sec = poll_sec
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        cleanup = getattr(self.store, "cleanup_expired", None)
        if cleanup is None:
            return 0
        return await cleanup(_now())

    async def start(self) -> None:
        self._stopped.clear()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except Exception:
                await logger.aexception("retention cleanup loop error")
            await asyncio.sleep(self.poll_sec)
