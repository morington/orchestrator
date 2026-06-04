from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class OutboxRecord:
    """
    Запись outbox — отложенная публикация в NATS.

    `kind=invoke` — вызов шага; `kind=workflow_completed` — событие завершения run.
    """

    run_id: str
    subject: str
    payload: dict[str, Any]
    delivery_id: str
    kind: str = "invoke"
    node_key: str | None = None
    status: str = "pending"
    outbox_id: int | None = None
    created_at: datetime | None = None
    sent_at: datetime | None = None
    locked_by: str | None = None
    locked_until: datetime | None = None


@dataclass
class DeadLetterRecord:
    """Запись dead-letter (poison / late / stale)."""

    subject: str
    reason: str
    payload: dict[str, Any] = field(default_factory=dict)
    run_id: str | None = None
    node_key: str | None = None
    created_at: datetime | None = None


@dataclass
class ClaimedNode:
    """Узел, захваченный с lease (для scheduler / timeout / outbox)."""

    run_id: str
    node_key: str
