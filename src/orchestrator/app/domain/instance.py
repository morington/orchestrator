from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from orchestrator.app.domain.contracts import COMPILED_GRAPH_VERSION, RUNTIME_VERSION
from orchestrator.core.entities import OutputSpec
from orchestrator.core.statuses import InstanceStatus


@dataclass
class WorkflowInstance:
    """
    Runtime-экземпляр (run) исполнения workflow.

    Идентичность: `definition_key` (тип) + `run_id` (прогон). Граф заморожен на
    `definition_hash`; пересборка для существующего run запрещена.
    """

    run_id: str
    definition_key: str
    idempotency_key: str
    definition_hash: str
    pipeline_version: str
    outputs_spec: dict[str, OutputSpec]
    definition_snapshot: dict[str, Any]

    instance_id: int | None = None
    business_ref: str | None = None
    definition_revision: str | None = None
    compiled_graph_version: str = COMPILED_GRAPH_VERSION
    runtime_version: str = RUNTIME_VERSION
    status: InstanceStatus = InstanceStatus.PENDING

    completion_published_at: datetime | None = None
    failure_reason: str | None = None
    outputs: dict[str, Any] = field(default_factory=dict)

    created_at: datetime | None = None
    updated_at: datetime | None = None
    locked_by: str | None = None
    locked_until: datetime | None = None
    revision: int = 0
