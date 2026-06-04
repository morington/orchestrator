from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from orchestrator.app.domain.graph import ExecutionEdge, ExecutionGraph, ExecutionNode
from orchestrator.app.domain.instance import WorkflowInstance
from orchestrator.app.domain.messages import InboxOutcome
from orchestrator.app.domain.records import DeadLetterRecord, OutboxRecord
from orchestrator.app.infrastructure.db.models import (
    DeadLetterModel,
    EdgeModel,
    InboxModel,
    InstanceModel,
    NodeModel,
    OutboxModel,
)
from orchestrator.core.entities import (
    DependsOnSpec,
    FilterEntity,
    MiddlewareEntity,
    OutputSpec,
    RetryPolicy,
)
from orchestrator.core.enums import EdgeKind, NodeType, OnFailure, TransportMode
from orchestrator.core.statuses import INSTANCE_TERMINAL_STATUSES, InstanceStatus, NodeStatus

_DEPENDS_ADAPTER = TypeAdapter(list[DependsOnSpec])
_FILTERS_ADAPTER = TypeAdapter(list[FilterEntity])
_OUTPUTS_ADAPTER = TypeAdapter(dict[str, OutputSpec])


class SqlAlchemyWorkflowStore:
    """
    WorkflowStore поверх SQLAlchemy 2.0 (PostgreSQL prod, SQLite для unit без concurrency).

    Атомарность node+outbox — в одной транзакции; claim_outbox использует
    `FOR UPDATE SKIP LOCKED` на PostgreSQL.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, dialect: str = "postgresql") -> None:
        self._session_factory = session_factory
        self._dialect = dialect

    # ------------------------------------------------------------- instance

    async def create_instance(
        self, instance: WorkflowInstance, graph: ExecutionGraph,
    ) -> tuple[WorkflowInstance, ExecutionGraph, bool]:
        async with self._session_factory() as session:
            try:
                async with session.begin():
                    model = self._instance_to_model(instance)
                    session.add(model)
                    await session.flush()
                    for node in graph.nodes.values():
                        session.add(self._node_to_model(model.id, node))
                    for edge in graph.edges:
                        session.add(
                            EdgeModel(
                                instance_id=model.id,
                                from_key=edge.from_key,
                                to_key=edge.to_key,
                                edge_kind=edge.edge_kind.value,
                                active=edge.active,
                            ),
                        )
                instance.instance_id = model.id
                return instance, graph, True
            except IntegrityError:
                await session.rollback()

        existing = await self.load_instance(instance.idempotency_key, by="idempotency_key")
        existing_graph = await self.load_graph(existing.run_id)
        return existing, existing_graph, False

    async def load_instance(self, run_id: str, *, by: str = "run_id") -> WorkflowInstance | None:
        column = InstanceModel.idempotency_key if by == "idempotency_key" else InstanceModel.run_id
        async with self._session_factory() as session:
            model = (await session.execute(select(InstanceModel).where(column == run_id))).scalar_one_or_none()
            return self._instance_from_model(model) if model else None

    async def load_graph(self, run_id: str) -> ExecutionGraph:
        async with self._session_factory() as session:
            instance = (
                await session.execute(select(InstanceModel).where(InstanceModel.run_id == run_id))
            ).scalar_one()
            nodes = (
                await session.execute(select(NodeModel).where(NodeModel.instance_id == instance.id))
            ).scalars().all()
            edges = (
                await session.execute(select(EdgeModel).where(EdgeModel.instance_id == instance.id))
            ).scalars().all()

        graph = ExecutionGraph()
        for node in nodes:
            graph.add_node(self._node_from_model(node))
        for edge in edges:
            graph.add_edge(
                ExecutionEdge(
                    from_key=edge.from_key,
                    to_key=edge.to_key,
                    edge_kind=EdgeKind(edge.edge_kind),
                    active=edge.active,
                ),
            )
        return graph

    async def persist_instance(self, instance: WorkflowInstance) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(InstanceModel)
                .where(InstanceModel.run_id == instance.run_id)
                .values(
                    status=instance.status.value,
                    outputs=instance.outputs,
                    failure_reason=instance.failure_reason,
                    completion_published_at=instance.completion_published_at,
                    revision=InstanceModel.revision + 1,
                ),
            )

    async def persist_node(self, run_id: str, node: ExecutionNode) -> None:
        async with self._session_factory() as session, session.begin():
            await self._update_node(session, run_id, node)

    async def enqueue_dispatch(self, run_id: str, node: ExecutionNode, outbox: OutboxRecord) -> None:
        async with self._session_factory() as session, session.begin():
            await self._update_node(session, run_id, node)
            session.add(self._outbox_to_model(outbox))

    async def enqueue_outbox(self, outbox: OutboxRecord) -> None:
        async with self._session_factory() as session, session.begin():
            session.add(self._outbox_to_model(outbox))

    # -------------------------------------------------------------- outbox

    async def claim_outbox(self, limit: int, owner: str, lease_ttl_sec: int) -> list[OutboxRecord]:
        async with self._session_factory() as session, session.begin():
            stmt = select(OutboxModel).where(OutboxModel.status == "pending").limit(limit)
            if self._dialect == "postgresql":
                stmt = stmt.with_for_update(skip_locked=True)
            models = (await session.execute(stmt)).scalars().all()
            for model in models:
                model.status = "claimed"
                model.locked_by = owner
            return [self._outbox_from_model(m) for m in models]

    async def complete_outbox(self, outbox_id: int, *, success: bool) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(OutboxModel)
                .where(OutboxModel.id == outbox_id)
                .values(status="sent" if success else "failed", sent_at=datetime.now(tz=None) if success else None),
            )

    # --------------------------------------------------------------- inbox

    async def register_inbox(self, step_run_id: str, attempt: int, message_id: str | None) -> InboxOutcome:
        async with self._session_factory() as session:
            try:
                async with session.begin():
                    session.add(
                        InboxModel(
                            step_run_id=step_run_id, attempt=attempt, message_id=message_id, outcome="applied",
                        ),
                    )
                return "applied"
            except IntegrityError:
                await session.rollback()
                return "duplicate"

    async def dead_letter(self, record: DeadLetterRecord) -> None:
        async with self._session_factory() as session, session.begin():
            session.add(
                DeadLetterModel(
                    subject=record.subject,
                    reason=record.reason,
                    payload=record.payload,
                    run_id=record.run_id,
                    node_key=record.node_key,
                ),
            )

    # ------------------------------------------------------ recovery / time

    async def claim_runnable_instances(self, limit: int, owner: str, lease_ttl_sec: int) -> list[str]:
        terminal = [s.value for s in INSTANCE_TERMINAL_STATUSES]
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(InstanceModel.run_id).where(InstanceModel.status.notin_(terminal)).limit(limit),
                )
            ).scalars().all()
            return list(rows)

    async def find_expired_nodes(self, now: datetime, limit: int) -> list[tuple[str, str]]:
        watched = [NodeStatus.WAITING_RESULT.value, NodeStatus.ENQUEUED.value, NodeStatus.DISPATCHED.value]
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(InstanceModel.run_id, NodeModel.node_key)
                    .join(NodeModel, NodeModel.instance_id == InstanceModel.id)
                    .where(NodeModel.status.in_(watched), NodeModel.deadline_at.is_not(None), NodeModel.deadline_at < now)
                    .limit(limit),
                )
            ).all()
            return [(run_id, node_key) for run_id, node_key in rows]

    async def reclaim_expired_leases(self, now: datetime) -> int:
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                update(NodeModel)
                .where(NodeModel.locked_until.is_not(None), NodeModel.locked_until < now)
                .values(locked_by=None, locked_until=None),
            )
            return result.rowcount or 0

    # -------------------------------------------------------------- helpers

    async def _update_node(self, session: AsyncSession, run_id: str, node: ExecutionNode) -> None:
        instance = (
            await session.execute(select(InstanceModel.id).where(InstanceModel.run_id == run_id))
        ).scalar_one()
        await session.execute(
            update(NodeModel)
            .where(NodeModel.instance_id == instance, NodeModel.node_key == node.node_key)
            .values(
                status=node.status.value,
                attempt=node.attempt,
                step_run_id=node.step_run_id,
                delivery_id=node.delivery_id,
                result=node.result,
                enqueued_at=node.enqueued_at,
                deadline_at=node.deadline_at,
                locked_by=node.locked_by,
                locked_until=node.locked_until,
                revision=NodeModel.revision + 1,
            ),
        )

    @staticmethod
    def _instance_to_model(instance: WorkflowInstance) -> InstanceModel:
        return InstanceModel(
            run_id=instance.run_id,
            definition_key=instance.definition_key,
            idempotency_key=instance.idempotency_key,
            business_ref=instance.business_ref,
            definition_hash=instance.definition_hash,
            definition_revision=instance.definition_revision,
            pipeline_version=instance.pipeline_version,
            compiled_graph_version=instance.compiled_graph_version,
            runtime_version=instance.runtime_version,
            status=instance.status.value,
            outputs_spec={k: v.model_dump(mode="json") for k, v in instance.outputs_spec.items()},
            definition_snapshot=instance.definition_snapshot,
            outputs=instance.outputs,
            failure_reason=instance.failure_reason,
            completion_published_at=instance.completion_published_at,
        )

    @staticmethod
    def _instance_from_model(model: InstanceModel) -> WorkflowInstance:
        return WorkflowInstance(
            instance_id=model.id,
            run_id=model.run_id,
            definition_key=model.definition_key,
            idempotency_key=model.idempotency_key,
            business_ref=model.business_ref,
            definition_hash=model.definition_hash,
            definition_revision=model.definition_revision,
            pipeline_version=model.pipeline_version,
            compiled_graph_version=model.compiled_graph_version,
            runtime_version=model.runtime_version,
            status=InstanceStatus(model.status),
            outputs_spec=_OUTPUTS_ADAPTER.validate_python(model.outputs_spec or {}),
            definition_snapshot=model.definition_snapshot or {},
            outputs=model.outputs or {},
            failure_reason=model.failure_reason,
            completion_published_at=model.completion_published_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
            revision=model.revision,
        )

    @staticmethod
    def _node_to_model(instance_id: int, node: ExecutionNode) -> NodeModel:
        return NodeModel(
            instance_id=instance_id,
            node_key=node.node_key,
            step_id=node.step_id,
            node_type=node.node_type.value,
            target=node.target,
            transport_mode=node.transport_mode.value,
            on_failure=node.on_failure.value,
            retry_policy=node.retry_policy.model_dump(mode="json"),
            valid_for_sec=node.valid_for_sec,
            data=node.data,
            filters=[f.model_dump(mode="json") for f in node.filters],
            middlewares=node.middlewares.model_dump(mode="json") if node.middlewares else None,
            depends_on=[d.model_dump(mode="json") for d in node.depends_on],
            status=node.status.value,
            attempt=node.attempt,
            step_run_id=node.step_run_id,
            delivery_id=node.delivery_id,
            result=node.result,
            enqueued_at=node.enqueued_at,
            deadline_at=node.deadline_at,
            compensation_status="none",
        )

    @staticmethod
    def _node_from_model(model: NodeModel) -> ExecutionNode:
        return ExecutionNode(
            node_key=model.node_key,
            step_id=model.step_id,
            node_type=NodeType(model.node_type),
            target=model.target,
            transport_mode=TransportMode(model.transport_mode),
            on_failure=OnFailure(model.on_failure),
            retry_policy=RetryPolicy.model_validate(model.retry_policy or {}),
            data=model.data or {},
            valid_for_sec=model.valid_for_sec,
            depends_on=_DEPENDS_ADAPTER.validate_python(model.depends_on or []),
            filters=_FILTERS_ADAPTER.validate_python(model.filters or []),
            middlewares=MiddlewareEntity.model_validate(model.middlewares) if model.middlewares else None,
            status=NodeStatus(model.status),
            attempt=model.attempt,
            step_run_id=model.step_run_id,
            delivery_id=model.delivery_id,
            result=model.result,
            enqueued_at=model.enqueued_at,
            deadline_at=model.deadline_at,
            locked_by=model.locked_by,
            locked_until=model.locked_until,
            revision=model.revision,
        )

    @staticmethod
    def _outbox_to_model(outbox: OutboxRecord) -> OutboxModel:
        return OutboxModel(
            run_id=outbox.run_id,
            node_key=outbox.node_key,
            subject=outbox.subject,
            payload=outbox.payload,
            delivery_id=outbox.delivery_id,
            kind=outbox.kind,
            status="pending",
        )

    @staticmethod
    def _outbox_from_model(model: OutboxModel) -> OutboxRecord:
        return OutboxRecord(
            run_id=model.run_id,
            subject=model.subject,
            payload=model.payload,
            delivery_id=model.delivery_id,
            kind=model.kind,
            node_key=model.node_key,
            status=model.status,
            outbox_id=model.id,
            created_at=model.created_at,
            sent_at=model.sent_at,
        )
