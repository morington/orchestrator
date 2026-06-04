from orchestrator.app.domain.ports.compensation import (
    CompensationContext,
    CompensationOutcome,
)
from orchestrator.core.statuses import InstanceStatus


class NoOpCompensationHandler:
    """
    Дефолтная компенсация: откатов нет, run переходит в FAILED.

    Единая точка расширения под Saga (roadmap); сейчас всегда возвращает FAILED без
    компенсирующих вызовов.
    """

    async def on_node_failed(self, ctx: CompensationContext) -> CompensationOutcome:
        return CompensationOutcome(instance_status=InstanceStatus.FAILED, runs=[])
