from typing import Any, Protocol


class MessagePublisher(Protocol):
    """
    Порт публикации в NATS.

    Возвращает True при успешной доставке (JetStream ack), False — при ошибке
    публикации (узел перейдёт в DISPATCH_ERROR). `msg_id` маппится в Nats-Msg-Id
    для дедупликации на стороне JetStream.
    """

    async def publish(self, subject: str, payload: dict[str, Any], *, msg_id: str | None = None) -> bool:
        """Опубликовать сообщение; вернуть успех доставки."""
        ...
