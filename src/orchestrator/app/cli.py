import asyncio
import contextlib
import signal

import structlog

from orchestrator.app.application.workers import (
    OutboxPublisher,
    RecoveryOnStartup,
    RetentionCleanupWorker,
    TimeoutWatcher,
)
from orchestrator.app.configuration.config import Configuration
from orchestrator.app.configuration.loggers import Loggers
from orchestrator.app.dependency_injection import build_container
from orchestrator.app.infrastructure.nats_gateway import NatsGateway

logger = structlog.getLogger(Loggers.main.name)


async def run() -> None:
    """Запустить оркестратор: recovery → gateway → воркеры; остановка по SIGINT/SIGTERM."""
    container = build_container()
    configuration = await container.get(Configuration)
    Loggers(developer_mode=configuration.is_development)

    recovery = await container.get(RecoveryOnStartup)
    gateway = await container.get(NatsGateway)
    outbox = await container.get(OutboxPublisher)
    timeout = await container.get(TimeoutWatcher)
    retention = await container.get(RetentionCleanupWorker)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    await gateway.start()
    await recovery.run()
    await outbox.start()
    await timeout.start()
    await retention.start()
    await logger.ainfo("orchestrator running", storage=configuration.storage.backend)

    try:
        await stop_event.wait()
    finally:
        await logger.awarning("orchestrator shutting down")
        await gateway.drain()
        await retention.stop()
        await timeout.stop()
        await outbox.stop()
        await container.close()


def main() -> None:
    """Точка входа консольного скрипта `orchestrator`."""
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.warning("orchestrator interrupted by user")


if __name__ == "__main__":
    main()
