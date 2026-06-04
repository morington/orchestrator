from collections.abc import AsyncIterable

import structlog
from dishka import Provider, Scope, provide
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from orchestrator.app.application.admin import AdminService
from orchestrator.app.application.metrics import Metrics
from orchestrator.app.application.runtime import WorkflowRuntime
from orchestrator.app.application.workers import (
    OutboxPublisher,
    RecoveryOnStartup,
    RetentionCleanupWorker,
    TimeoutWatcher,
)
from orchestrator.app.configuration.config import Configuration
from orchestrator.app.configuration.loggers import Loggers
from orchestrator.app.domain.ports.broker import MessagePublisher
from orchestrator.app.domain.ports.store import WorkflowStore
from orchestrator.app.infrastructure.db.base import Base
from orchestrator.app.infrastructure.nats_gateway import NatsGateway
from orchestrator.app.infrastructure.store_memory import InMemoryWorkflowStore
from orchestrator.app.infrastructure.store_sql import SqlAlchemyWorkflowStore
from orchestrator.app.presentation.handlers import NatsHandlers

logger = structlog.getLogger(Loggers.providers.name)


class ConfigurationProvider(Provider):
    scope = Scope.APP

    @provide
    def configuration(self) -> Configuration:
        return Configuration()


class StoreProvider(Provider):
    scope = Scope.APP

    @provide
    async def engine(self, configuration: Configuration) -> AsyncIterable[AsyncEngine | None]:
        if configuration.storage.is_memory:
            yield None
            return
        engine = create_async_engine(configuration.storage.url, pool_pre_ping=True)
        if configuration.storage.is_sqlite:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        yield engine
        await engine.dispose()

    @provide
    def store(self, configuration: Configuration, engine: AsyncEngine | None) -> WorkflowStore:
        if engine is None:
            return InMemoryWorkflowStore()
        dialect = "sqlite" if configuration.storage.is_sqlite else "postgresql"
        factory = async_sessionmaker(engine, expire_on_commit=False)
        return SqlAlchemyWorkflowStore(factory, dialect=dialect)


class ServiceProvider(Provider):
    scope = Scope.APP

    @provide
    def metrics(self) -> Metrics:
        return Metrics()

    @provide
    def runtime(self, store: WorkflowStore, configuration: Configuration) -> WorkflowRuntime:
        return WorkflowRuntime(store, completed_subject=configuration.subjects.workflow_completed)

    @provide
    def admin(self, runtime: WorkflowRuntime) -> AdminService:
        return AdminService(runtime)

    @provide
    def handlers(self, runtime: WorkflowRuntime, admin: AdminService, metrics: Metrics) -> NatsHandlers:
        return NatsHandlers(runtime, admin, metrics)

    @provide
    def gateway(self, configuration: Configuration, handlers: NatsHandlers) -> NatsGateway:
        return NatsGateway(configuration, handlers)

    @provide
    def publisher(self, gateway: NatsGateway) -> MessagePublisher:
        return gateway

    @provide
    def outbox_publisher(
        self, store: WorkflowStore, publisher: MessagePublisher, runtime: WorkflowRuntime, configuration: Configuration,
    ) -> OutboxPublisher:
        engine = configuration.engine
        return OutboxPublisher(
            store,
            publisher,
            runtime,
            owner=engine.replica_id,
            batch=engine.outbox_batch,
            lease_ttl_sec=engine.lease_ttl_sec,
            poll_sec=engine.outbox_poll_sec,
        )

    @provide
    def timeout_watcher(
        self, store: WorkflowStore, runtime: WorkflowRuntime, configuration: Configuration,
    ) -> TimeoutWatcher:
        engine = configuration.engine
        return TimeoutWatcher(store, runtime, batch=engine.scheduler_batch, poll_sec=engine.timeout_poll_sec)

    @provide
    def retention_worker(self, store: WorkflowStore, configuration: Configuration) -> RetentionCleanupWorker:
        return RetentionCleanupWorker(store, poll_sec=configuration.retention.poll_sec)

    @provide
    def recovery(
        self, store: WorkflowStore, outbox_publisher: OutboxPublisher, timeout_watcher: TimeoutWatcher,
    ) -> RecoveryOnStartup:
        return RecoveryOnStartup(store, outbox_publisher, timeout_watcher)
