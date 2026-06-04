from dataclasses import dataclass, field
from typing import Any, Protocol

from orchestrator.app.domain.graph import ExecutionNode
from orchestrator.app.domain.instance import WorkflowInstance
from orchestrator.core.statuses import InstanceStatus


@dataclass(frozen=True)
class CompensationContext:
    """Контекст вызова компенсации при отказе узла."""

    instance: WorkflowInstance
    failed_node: ExecutionNode
    completed_nodes: list[ExecutionNode]


@dataclass(frozen=True)
class CompensationRun:
    """Описание компенсирующего вызова (roadmap)."""

    target: str
    data: dict[str, Any]


@dataclass(frozen=True)
class CompensationOutcome:
    """Результат компенсации: новый статус instance и список компенсаций."""

    instance_status: InstanceStatus
    runs: list[CompensationRun] = field(default_factory=list)


class CompensationHandler(Protocol):
    """Единая точка расширения Saga/Compensation. В 1.0 — NoOp (instance FAILED)."""

    async def on_node_failed(self, ctx: CompensationContext) -> CompensationOutcome:
        """Решить судьбу run при отказе узла."""
        ...
