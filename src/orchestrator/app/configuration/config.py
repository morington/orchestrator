from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from structlog import getLogger

from orchestrator.app.configuration.loggers import Loggers

logger = getLogger(Loggers.development.name)


class StorageModel(BaseModel):
    """Конфигурация хранилища состояния (порт WorkflowStore)."""

    backend: str = Field(default="postgresql", description="postgresql | sqlite | memory")
    url: str = Field(
        default="postgresql+asyncpg://orchestrator:orchestrator@localhost:5432/orchestrator",
        description="SQLAlchemy async DSN для SQL-семейства",
    )
    payload_inline_max: int = Field(default=262144, ge=1, description="Порог inline payload, байт")

    @property
    def is_memory(self) -> bool:
        return self.backend == "memory"

    @property
    def is_sqlite(self) -> bool:
        return self.backend == "sqlite"


class NatsModel(BaseModel):
    """Подключение к NATS / JetStream."""

    servers: list[str] = Field(default_factory=lambda: ["nats://localhost:4222"])
    stream: str = Field(default="ORCHESTRATOR")
    durable_prefix: str = Field(default="orchestrator")
    connect_timeout: float = Field(default=5.0, gt=0)
    publish_timeout: float = Field(default=5.0, gt=0)
    ack_wait: float = Field(default=60.0, gt=0)


class SubjectsModel(BaseModel):
    """Канонические NATS subjects (см. docs/CONTRACTS.md)."""

    workflow_start: str = Field(default="orchestrator.workflow.start")
    results: str = Field(default="orchestrator.results")
    workflow_completed: str = Field(default="orchestrator.workflow.completed")
    workflow_cancel: str = Field(default="orchestrator.workflow.cancel")
    admin_prefix: str = Field(default="orchestrator.admin")
    deadletter: str = Field(default="orchestrator.deadletter")


class EngineModel(BaseModel):
    """Параметры runtime, конкурентности и outbox."""

    replica_id: str = Field(default="orchestrator-1", description="ID реплики для leases")
    lease_ttl_sec: int = Field(default=30, ge=1, description="TTL lease на узел")
    outbox_batch: int = Field(default=50, ge=1)
    scheduler_batch: int = Field(default=50, ge=1)
    outbox_poll_sec: float = Field(default=1.0, gt=0)
    timeout_poll_sec: float = Field(default=5.0, gt=0)
    allowed_target_prefixes: list[str] = Field(
        default_factory=lambda: ["service.", "meta."],
        description="Разрешённые префиксы meta.target",
    )


class RetentionModel(BaseModel):
    """Политика очистки данных (RetentionCleanupWorker)."""

    workflow_days: int = Field(default=90, ge=1)
    outbox_days: int = Field(default=7, ge=1)
    inbox_days: int = Field(default=30, ge=1)
    dlq_days: int = Field(default=90, ge=1)
    poll_sec: float = Field(default=3600.0, gt=0)


class OopsysAgentModel(BaseModel):
    """Справочная конфигурация локального oopsys-агента (.env / документация)."""

    enabled: bool = Field(default=False)
    host: str = Field(default="127.0.0.1")
    port: int = Field(default=8080, ge=1, le=65535)
    path: str = Field(default="/agents/ingest")


class Configuration(BaseSettings):
    """Корневая конфигурация оркестратора (pydantic-settings, nested `__`)."""

    is_development: bool = Field(default=False, alias="DEV")

    storage: StorageModel = StorageModel()
    nats: NatsModel = NatsModel()
    subjects: SubjectsModel = SubjectsModel()
    engine: EngineModel = EngineModel()
    retention: RetentionModel = RetentionModel()
    oopsys_agent: OopsysAgentModel = OopsysAgentModel()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    @model_validator(mode="after")
    def warn_development(self) -> "Configuration":
        if self.is_development:
            logger.warning("Application started in development mode")
        return self
