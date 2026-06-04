from typing import Any

import structlog

from orchestrator.app.application.admin import AdminError, AdminService
from orchestrator.app.application.metrics import Metrics
from orchestrator.app.application.runtime import WorkflowRuntime
from orchestrator.app.configuration.loggers import Loggers
from orchestrator.app.domain.contracts import RUNTIME_VERSION
from orchestrator.core.errors import UnsupportedVersionError, WorkflowDefinitionError

logger = structlog.getLogger(Loggers.handlers.name)


class NatsHandlers:
    """
    Тонкий слой presentation: десериализованное тело → runtime/admin → ответ/ack.

    Бизнес-логики нет; ошибки версий/валидации возвращаются вызывающему, poison-сообщения
    уходят в DLQ внутри runtime (ack вместо nack-loop).
    """

    def __init__(self, runtime: WorkflowRuntime, admin: AdminService, metrics: Metrics | None = None) -> None:
        self.runtime = runtime
        self.admin = admin
        self.metrics = metrics or Metrics()

    async def handle_start(self, body: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self.runtime.start(body)
        except UnsupportedVersionError as exc:
            self.metrics.incr("workflows_rejected", message_version=str(body.get("message_version")))
            await logger.awarning("start rejected: unsupported version", reason=str(exc))
            return {"error": "unsupported_version", "detail": str(exc)}
        except WorkflowDefinitionError as exc:
            self.metrics.incr("workflows_rejected")
            await logger.awarning("start rejected: invalid definition", reason=str(exc))
            return {"error": "invalid_definition", "detail": str(exc)}
        self.metrics.incr("workflows_created" if not response.deduplicated else "workflows_deduplicated")
        return response.model_dump(mode="json")

    async def handle_result(self, body: dict[str, Any]) -> None:
        await self.runtime.on_result(body)
        self.metrics.incr("results_processed")

    async def handle_cancel(self, body: dict[str, Any]) -> dict[str, Any]:
        run_id = body.get("run_id")
        cancelled = await self.runtime.cancel(str(run_id), reason=body.get("reason"))
        self.metrics.incr("workflows_cancelled" if cancelled else "workflows_cancel_noop")
        return {"runtime_version": RUNTIME_VERSION, "run_id": run_id, "cancelled": cancelled}

    async def handle_admin(self, command: str, body: dict[str, Any]) -> dict[str, Any]:
        run_id = str(body.get("run_id"))
        try:
            if command == "inspect":
                return await self.admin.inspect(run_id)
            if command == "retry_node":
                return await self.admin.retry_node(run_id, str(body["node_key"]))
            if command == "resume":
                return await self.admin.resume(run_id)
            if command == "abandon_node":
                return await self.admin.abandon_node(run_id, str(body["node_key"]))
            if command == "cancel":
                return await self.admin.cancel(run_id, body.get("reason"))
        except AdminError as exc:
            return {"error": "admin_error", "detail": str(exc)}
        return {"error": "unknown_command", "detail": command}
