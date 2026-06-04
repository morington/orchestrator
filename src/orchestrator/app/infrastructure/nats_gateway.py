import contextlib
from typing import Any

import structlog
from faststream import AckPolicy
from faststream.nats import JStream, NatsBroker
from nats.js.api import ConsumerConfig

from orchestrator.app.configuration.config import Configuration
from orchestrator.app.configuration.loggers import Loggers
from orchestrator.app.presentation.handlers import NatsHandlers

logger = structlog.getLogger(Loggers.nats.name)

_ADMIN_COMMANDS = ("inspect", "retry_node", "resume", "abandon_node", "cancel")


class NatsGateway:
    """
    Шлюз NATS: ingress pull-подписчики (start/results/cancel/admin) на JetStream и egress
    publish с `Nats-Msg-Id` для дедупликации. Реализует порт MessagePublisher.

    В dev (DEV=true) допускается Core publish без durable-гарантий.
    """

    def __init__(self, config: Configuration, handlers: NatsHandlers) -> None:
        self._config = config
        self._handlers = handlers
        self._broker = NatsBroker(config.nats.servers, connect_timeout=int(config.nats.connect_timeout), logger=None)
        self._connected = False
        self._register_subscribers()

    @property
    def broker(self) -> NatsBroker:
        return self._broker

    def _stream(self) -> JStream:
        return JStream(name=self._config.nats.stream, subjects=["orchestrator.>"], declare=True)

    def _register_subscribers(self) -> None:
        config = self._config
        subjects = config.subjects
        stream = self._stream()
        durable = config.nats.durable_prefix
        consumer = ConsumerConfig(ack_wait=config.nats.ack_wait, max_deliver=-1)

        def _subscribe(subject: str, suffix: str):
            return self._broker.subscriber(
                subject,
                stream=stream,
                durable=f"{durable}-{suffix}",
                pull_sub=True,
                ack_policy=AckPolicy.NACK_ON_ERROR,
                config=consumer,
            )

        @_subscribe(subjects.workflow_start, "start")
        async def _on_start(body: dict[str, Any]) -> dict[str, Any]:
            return await self._handlers.handle_start(body)

        @_subscribe(subjects.results, "results")
        async def _on_result(body: dict[str, Any]) -> None:
            await self._handlers.handle_result(body)

        @_subscribe(subjects.workflow_cancel, "cancel")
        async def _on_cancel(body: dict[str, Any]) -> dict[str, Any]:
            return await self._handlers.handle_cancel(body)

        for command in _ADMIN_COMMANDS:
            self._register_admin(command, stream, durable, consumer)

    def _register_admin(self, command: str, stream: JStream, durable: str, consumer: ConsumerConfig) -> None:
        subject = f"{self._config.subjects.admin_prefix}.{command}"

        @self._broker.subscriber(
            subject,
            stream=stream,
            durable=f"{durable}-admin-{command}",
            pull_sub=True,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            config=consumer,
        )
        async def _on_admin(body: dict[str, Any]) -> dict[str, Any]:
            return await self._handlers.handle_admin(command, body)

    async def publish(self, subject: str, payload: dict[str, Any], *, msg_id: str | None = None) -> bool:
        """Egress publish (порт MessagePublisher). True при успешной доставке."""
        if not self._connected:
            return False
        headers = {"Nats-Msg-Id": msg_id} if msg_id else None
        try:
            await self._broker.publish(
                payload, subject, timeout=self._config.nats.publish_timeout, headers=headers,
            )
        except Exception as exc:
            await logger.aerror("egress publish failed", subject=subject, reason=str(exc))
            return False
        return True

    async def start(self) -> None:
        await self._broker.connect()
        await self._broker.start()
        self._connected = True
        await logger.ainfo("nats gateway ready", servers=self._config.nats.servers, stream=self._config.nats.stream)

    async def drain(self) -> None:
        """Drain NATS перед остановкой (уменьшает orphaned state вместе с recovery)."""
        self._connected = False
        with contextlib.suppress(Exception):
            await self._broker.stop()
