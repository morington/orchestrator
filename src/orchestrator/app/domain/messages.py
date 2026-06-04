from typing import Any, Literal

from pydantic import BaseModel, Field

from orchestrator.app.domain.contracts import CURRENT_MESSAGE_VERSION
from orchestrator.core.enums import FailureClass, TransportMode


class InvokeEnvelope(BaseModel):
    """Исходящий вызов шага в микросервис (subject = meta.target)."""

    message_version: str = Field(default=CURRENT_MESSAGE_VERSION)
    definition_key: str
    run_id: str
    node_key: str
    step_run_id: str
    attempt: int = Field(..., ge=1)
    transport_mode: TransportMode = TransportMode.ASYNC_RESULT_SUBJECT
    reply_subject: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class StepError(BaseModel):
    """Ошибка в результате шага."""

    failure_class: FailureClass = FailureClass.BUSINESS
    message: str | None = None


class StepResultMessage(BaseModel):
    """Входящий результат шага на orchestrator.results."""

    message_version: str
    definition_key: str
    run_id: str
    node_key: str
    step_run_id: str
    attempt: int = Field(..., ge=1)
    result: dict[str, Any] | None = None
    error: StepError | None = None


class WorkflowCompletedMessage(BaseModel):
    """Событие завершения run (ровно один раз на run_id)."""

    message_version: str = Field(default=CURRENT_MESSAGE_VERSION)
    definition_key: str
    run_id: str
    status: str
    outputs: dict[str, Any] = Field(default_factory=dict)
    failure_reason: str | None = None


class CancelCommand(BaseModel):
    """Команда отмены run."""

    message_version: str
    definition_key: str
    run_id: str
    reason: str | None = None


class StartResponse(BaseModel):
    """Ответ на orchestrator.workflow.start."""

    message_version: str = Field(default=CURRENT_MESSAGE_VERSION)
    definition_key: str
    run_id: str
    instance_status: str
    deduplicated: bool = False


InboxOutcome = Literal["applied", "duplicate", "stale", "late_result", "rejected"]
