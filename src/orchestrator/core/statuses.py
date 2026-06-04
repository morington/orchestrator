from enum import StrEnum


class NodeStatus(StrEnum):
    """
    Статус узла графа исполнения (строгий FSM, см. docs/SEMANTICS.md §2/§4).

    Не-terminal: PENDING, RUNNABLE, ENQUEUED, DISPATCHED, WAITING_RESULT, DISPATCH_ERROR, CANCELLING.
    Terminal (CLOSED): COMPLETED, SKIPPED, SKIPPED_BY_GOTO, ABANDONED, EXPIRED, FAILED, CANCELLED, DISPATCH_FAILED.
    """

    PENDING = "pending"
    RUNNABLE = "runnable"
    ENQUEUED = "enqueued"
    DISPATCHED = "dispatched"
    WAITING_RESULT = "waiting_result"
    DISPATCH_ERROR = "dispatch_error"
    CANCELLING = "cancelling"

    COMPLETED = "completed"
    SKIPPED = "skipped"
    SKIPPED_BY_GOTO = "skipped_by_goto"
    ABANDONED = "abandoned"
    EXPIRED = "expired"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DISPATCH_FAILED = "dispatch_failed"


class InstanceStatus(StrEnum):
    """
    Статус run (экземпляра исполнения workflow).

    Compensation-статусы зарезервированы под roadmap и в 1.0 не используются.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"

    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"


class CompensationStatus(StrEnum):
    """
    Статус компенсации узла (задел Saga, в 1.0 всегда NONE)."""

    NONE = "none"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


SUCCESS_STATUSES: frozenset[NodeStatus] = frozenset({NodeStatus.COMPLETED})

CLOSED_STATUSES: frozenset[NodeStatus] = frozenset(
    {
        NodeStatus.COMPLETED,
        NodeStatus.SKIPPED,
        NodeStatus.SKIPPED_BY_GOTO,
        NodeStatus.ABANDONED,
        NodeStatus.EXPIRED,
        NodeStatus.FAILED,
        NodeStatus.CANCELLED,
        NodeStatus.DISPATCH_FAILED,
    },
)

INSTANCE_TERMINAL_STATUSES: frozenset[InstanceStatus] = frozenset(
    {
        InstanceStatus.COMPLETED,
        InstanceStatus.FAILED,
        InstanceStatus.CANCELLED,
        InstanceStatus.COMPENSATED,
        InstanceStatus.COMPENSATION_FAILED,
    },
)


def is_success(status: NodeStatus) -> bool:
    """Узел успешно завершён."""
    return status in SUCCESS_STATUSES


def is_closed(status: NodeStatus) -> bool:
    """Узел в terminal-статусе (входит в CLOSED)."""
    return status in CLOSED_STATUSES


def is_instance_terminal(status: InstanceStatus) -> bool:
    """Run в terminal-статусе."""
    return status in INSTANCE_TERMINAL_STATUSES
