from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from orchestrator.app.infrastructure.db.base import Base, JsonType, PkType


class InstanceModel(Base):
    __tablename__ = "workflow_instances"

    id: Mapped[int] = mapped_column(PkType, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    definition_key: Mapped[str] = mapped_column(String(255), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    business_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)

    definition_hash: Mapped[str] = mapped_column(String(64))
    definition_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pipeline_version: Mapped[str] = mapped_column(String(16))
    compiled_graph_version: Mapped[str] = mapped_column(String(16))
    runtime_version: Mapped[str] = mapped_column(String(16))

    status: Mapped[str] = mapped_column(String(32), index=True)
    outputs_spec: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    definition_snapshot: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    outputs: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    failure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    completion_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),
    )
    locked_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)

    nodes: Mapped[list["NodeModel"]] = relationship(back_populates="instance", cascade="all, delete-orphan")
    edges: Mapped[list["EdgeModel"]] = relationship(back_populates="instance", cascade="all, delete-orphan")


class NodeModel(Base):
    __tablename__ = "execution_nodes"
    __table_args__ = (UniqueConstraint("instance_id", "node_key", name="uq_node_key"),)

    id: Mapped[int] = mapped_column(PkType, primary_key=True, autoincrement=True)
    instance_id: Mapped[int] = mapped_column(ForeignKey("workflow_instances.id", ondelete="CASCADE"), index=True)

    node_key: Mapped[str] = mapped_column(String(64))
    step_id: Mapped[int] = mapped_column(Integer)
    node_type: Mapped[str] = mapped_column(String(32))
    target: Mapped[str] = mapped_column(String(255))
    transport_mode: Mapped[str] = mapped_column(String(32))
    on_failure: Mapped[str] = mapped_column(String(16))
    retry_policy: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    valid_for_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    data: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    filters: Mapped[list] = mapped_column(JsonType, default=list)
    middlewares: Mapped[dict[str, Any] | None] = mapped_column(JsonType, nullable=True)
    depends_on: Mapped[list] = mapped_column(JsonType, default=list)

    status: Mapped[str] = mapped_column(String(32), index=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    step_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    delivery_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JsonType, nullable=True)
    enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    locked_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    compensation_status: Mapped[str] = mapped_column(String(16), default="none")

    instance: Mapped[InstanceModel] = relationship(back_populates="nodes")


class EdgeModel(Base):
    __tablename__ = "execution_edges"

    id: Mapped[int] = mapped_column(PkType, primary_key=True, autoincrement=True)
    instance_id: Mapped[int] = mapped_column(ForeignKey("workflow_instances.id", ondelete="CASCADE"), index=True)
    from_key: Mapped[str] = mapped_column(String(64))
    to_key: Mapped[str] = mapped_column(String(64))
    edge_kind: Mapped[str] = mapped_column(String(16))
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    instance: Mapped[InstanceModel] = relationship(back_populates="edges")


class JoinStateModel(Base):
    __tablename__ = "join_states"

    id: Mapped[int] = mapped_column(PkType, primary_key=True, autoincrement=True)
    instance_id: Mapped[int] = mapped_column(ForeignKey("workflow_instances.id", ondelete="CASCADE"), index=True)
    node_key: Mapped[str] = mapped_column(String(64))
    expected: Mapped[int] = mapped_column(Integer, default=0)
    arrived: Mapped[int] = mapped_column(Integer, default=0)


class OutboxModel(Base):
    __tablename__ = "outbox_messages"
    __table_args__ = (
        UniqueConstraint("delivery_id", name="uq_outbox_delivery"),
        Index("ix_outbox_status_created", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(PkType, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    node_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    subject: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    delivery_id: Mapped[str] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String(32), default="invoke")
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class InboxModel(Base):
    __tablename__ = "inbox_messages"
    __table_args__ = (UniqueConstraint("step_run_id", "attempt", name="uq_inbox_step_attempt"),)

    id: Mapped[int] = mapped_column(PkType, primary_key=True, autoincrement=True)
    step_run_id: Mapped[str] = mapped_column(String(64), index=True)
    attempt: Mapped[int] = mapped_column(Integer)
    message_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    outcome: Mapped[str] = mapped_column(String(16), default="applied")
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DeadLetterModel(Base):
    __tablename__ = "dead_letter_messages"

    id: Mapped[int] = mapped_column(PkType, primary_key=True, autoincrement=True)
    subject: Mapped[str] = mapped_column(String(255))
    reason: Mapped[str] = mapped_column(String(64), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonType, default=dict)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    node_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
