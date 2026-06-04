from orchestrator.core.errors import InvalidTransitionError
from orchestrator.core.statuses import CLOSED_STATUSES, NodeStatus

# Строгая матрица переходов узла (docs/SEMANTICS.md §4 + production §5).
_NODE_TRANSITIONS: dict[NodeStatus, frozenset[NodeStatus]] = {
    NodeStatus.PENDING: frozenset(
        {NodeStatus.RUNNABLE, NodeStatus.SKIPPED, NodeStatus.SKIPPED_BY_GOTO, NodeStatus.CANCELLED},
    ),
    NodeStatus.RUNNABLE: frozenset(
        {NodeStatus.ENQUEUED, NodeStatus.SKIPPED, NodeStatus.SKIPPED_BY_GOTO, NodeStatus.CANCELLED},
    ),
    NodeStatus.ENQUEUED: frozenset(
        {NodeStatus.DISPATCHED, NodeStatus.CANCELLED, NodeStatus.EXPIRED},
    ),
    NodeStatus.DISPATCHED: frozenset(
        {
            NodeStatus.WAITING_RESULT,
            NodeStatus.COMPLETED,  # fire_and_forget ack ok
            NodeStatus.DISPATCH_ERROR,
            NodeStatus.EXPIRED,
        },
    ),
    NodeStatus.WAITING_RESULT: frozenset(
        {
            NodeStatus.COMPLETED,
            NodeStatus.FAILED,
            NodeStatus.ABANDONED,
            NodeStatus.EXPIRED,
            NodeStatus.SKIPPED,
            NodeStatus.CANCELLING,
            NodeStatus.ENQUEUED,  # повтор при транзиентном результате (новый attempt)
        },
    ),
    NodeStatus.DISPATCH_ERROR: frozenset(
        {NodeStatus.ENQUEUED, NodeStatus.DISPATCH_FAILED, NodeStatus.EXPIRED},
    ),
    NodeStatus.CANCELLING: frozenset({NodeStatus.CANCELLED}),
}


class NodeStateMachine:
    """
    Строгий автомат переходов статуса узла.

    Любой переход вне матрицы приводит к InvalidTransitionError; terminal-статусы
    (CLOSED) не имеют исходящих переходов.
    """

    @staticmethod
    def can_transition(current: NodeStatus, target: NodeStatus) -> bool:
        """Проверить, допустим ли переход current → target."""
        return target in _NODE_TRANSITIONS.get(current, frozenset())

    @staticmethod
    def transition(current: NodeStatus, target: NodeStatus) -> NodeStatus:
        """
        Выполнить переход current → target.

        Args:
            current: Текущий статус узла.
            target: Желаемый статус.

        Returns:
            NodeStatus: target при допустимом переходе.

        Raises:
            InvalidTransitionError: Если переход не разрешён матрицей.
        """
        if not NodeStateMachine.can_transition(current, target):
            reason = "terminal status has no transitions" if current in CLOSED_STATUSES else "transition not allowed"
            raise InvalidTransitionError(f"{current} -> {target}: {reason}")
        return target

    @staticmethod
    def allowed_targets(current: NodeStatus) -> frozenset[NodeStatus]:
        """Множество допустимых целевых статусов из current."""
        return _NODE_TRANSITIONS.get(current, frozenset())
