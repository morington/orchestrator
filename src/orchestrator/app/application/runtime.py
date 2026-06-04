from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from orchestrator.app.application.compensation import NoOpCompensationHandler
from orchestrator.app.application.compiler import GraphCompiler
from orchestrator.app.application.ids import new_delivery_id, new_step_run_id
from orchestrator.app.application.scheduler import DagGraphScheduler
from orchestrator.app.configuration.loggers import Loggers
from orchestrator.app.domain.contracts import (
    CURRENT_MESSAGE_VERSION,
    is_supported_message_version,
)
from orchestrator.app.domain.graph import ExecutionGraph, ExecutionNode
from orchestrator.app.domain.instance import WorkflowInstance
from orchestrator.app.domain.messages import (
    InvokeEnvelope,
    StartResponse,
    StepResultMessage,
    WorkflowCompletedMessage,
)
from orchestrator.app.domain.ports.compensation import CompensationContext, CompensationHandler
from orchestrator.app.domain.ports.store import WorkflowStore
from orchestrator.app.domain.records import DeadLetterRecord, OutboxRecord
from orchestrator.core.dependency import Dependency
from orchestrator.core.entities import (
    EndFilterAction,
    ErrorFilterAction,
    GotoFilterAction,
    SkipFilterAction,
    WorkflowDefinition,
)
from orchestrator.core.enums import EdgeKind, FailureClass, OnFailure, TransportMode
from orchestrator.core.errors import UnsupportedVersionError
from orchestrator.core.filter import ConditionEvaluator
from orchestrator.core.middleware import MiddlewareExecutor
from orchestrator.core.state_machine import NodeStateMachine
from orchestrator.core.statuses import InstanceStatus, NodeStatus, is_closed
from orchestrator.core.validator import WorkflowDefinitionValidator

logger = structlog.getLogger(Loggers.engine.name)


def _now() -> datetime:
    return datetime.now(tz=UTC)


class WorkflowRuntime:
    """
    Ядро исполнения: старт run, диспетч узлов через outbox, обработка результатов,
    фильтры/GOTO, отказоустойчивость и финализация (workflow.completed ровно один раз).

    Зависит только от портов (WorkflowStore, scheduler, compensation), не от NATS/SQL.
    """

    def __init__(
        self,
        store: WorkflowStore,
        *,
        scheduler: DagGraphScheduler | None = None,
        compiler: GraphCompiler | None = None,
        validator: WorkflowDefinitionValidator | None = None,
        compensation: CompensationHandler | None = None,
        completed_subject: str = "orchestrator.workflow.completed",
    ) -> None:
        self.store = store
        self.scheduler = scheduler or DagGraphScheduler()
        self.compiler = compiler or GraphCompiler()
        self.validator = validator or WorkflowDefinitionValidator()
        self.compensation = compensation or NoOpCompensationHandler()
        self.completed_subject = completed_subject
        self._evaluator = ConditionEvaluator()

    # ------------------------------------------------------------------ start

    async def start(self, raw: dict[str, Any]) -> StartResponse:
        """
        Обработать orchestrator.workflow.start: версия → валидация → компиляция →
        идемпотентное создание run → планирование корневых узлов.
        """
        message_version = raw.get("message_version", CURRENT_MESSAGE_VERSION)
        if not is_supported_message_version(message_version):
            raise UnsupportedVersionError(f"unsupported message_version '{message_version}'")

        definition = WorkflowDefinition.model_validate({k: v for k, v in raw.items() if k != "message_version"})
        self.validator.validate(definition)

        instance, graph = self.compiler.compile(definition)
        instance, graph, created = await self.store.create_instance(instance, graph)

        if not created:
            await logger.ainfo("start deduplicated", run_id=instance.run_id, definition_key=instance.definition_key)
            return StartResponse(
                definition_key=instance.definition_key,
                run_id=instance.run_id,
                instance_status=instance.status.value,
                deduplicated=True,
            )

        instance.status = InstanceStatus.RUNNING
        await self.store.persist_instance(instance)
        await self.schedule(instance, graph)

        return StartResponse(
            definition_key=instance.definition_key,
            run_id=instance.run_id,
            instance_status=instance.status.value,
        )

    # --------------------------------------------------------------- schedule

    async def schedule(self, instance: WorkflowInstance, graph: ExecutionGraph) -> None:
        """
        Продвинуть run: каскадно пропустить заблокированные узлы, применить фильтры
        к готовым узлам и поставить остальные в outbox; затем проверить завершение.
        """
        if instance.status not in (InstanceStatus.RUNNING, InstanceStatus.PENDING):
            return

        progressed = True
        while progressed:
            progressed = False

            for node in self.scheduler.list_unreachable(graph):
                if not self._has_active_goto_into(graph, node):
                    await self._set_node(instance, node, NodeStatus.SKIPPED)
                    progressed = True

            for node in self.scheduler.list_runnable(graph) + self._goto_runnable(graph):
                if node.status != NodeStatus.PENDING:
                    continue
                acted = await self._evaluate_and_dispatch(instance, graph, node)
                progressed = progressed or acted
                if instance.status in (InstanceStatus.FAILED, InstanceStatus.COMPLETED):
                    return

        await self._maybe_finalize(instance, graph)

    def _goto_runnable(self, graph: ExecutionGraph) -> list[ExecutionNode]:
        return [
            node
            for node in graph.nodes.values()
            if node.status == NodeStatus.PENDING and self._has_active_goto_into(graph, node)
        ]

    @staticmethod
    def _has_active_goto_into(graph: ExecutionGraph, node: ExecutionNode) -> bool:
        for edge in graph.edges:
            if edge.to_key == node.node_key and edge.edge_kind == EdgeKind.GOTO and edge.active:
                source = graph.nodes.get(edge.from_key)
                if source is not None and is_closed(source.status):
                    return True
        return False

    async def _evaluate_and_dispatch(
        self, instance: WorkflowInstance, graph: ExecutionGraph, node: ExecutionNode,
    ) -> bool:
        action = self._evaluate_filters(graph, node)
        if action is not None:
            await self._apply_filter_action(instance, graph, node, action)
            return True
        await self._dispatch(instance, node)
        return True

    # ---------------------------------------------------------------- filters

    def _evaluate_filters(self, graph: ExecutionGraph, node: ExecutionNode) -> Any:
        if not node.filters:
            return None
        global_data = self._results_by_step(graph)
        for flt in node.filters:
            if self._evaluator.evaluate(flt.condition, global_data=global_data, local=node.data):
                return flt
        return None

    async def _apply_filter_action(
        self, instance: WorkflowInstance, graph: ExecutionGraph, node: ExecutionNode, flt: Any,
    ) -> None:
        if isinstance(flt, SkipFilterAction):
            await self._set_node(instance, node, NodeStatus.SKIPPED)
        elif isinstance(flt, EndFilterAction):
            await self._set_node(instance, node, NodeStatus.SKIPPED)
            await self._complete_instance(instance, graph)
        elif isinstance(flt, ErrorFilterAction):
            await self._set_node(instance, node, NodeStatus.SKIPPED)
            await self._fail_instance(instance, graph, node, reason="filter_error")
        elif isinstance(flt, GotoFilterAction):
            await self._apply_goto(instance, graph, node, flt.targets)

    async def _apply_goto(
        self, instance: WorkflowInstance, graph: ExecutionGraph, node: ExecutionNode, targets: list[int],
    ) -> None:
        target_keys = {f"step-{t}" for t in targets}
        await self._set_node(instance, node, NodeStatus.SKIPPED)

        for edge in graph.goto_edges_from(node.node_key):
            if edge.to_key in target_keys:
                edge.active = True

        bypass = self._downstream_closure(graph, node.node_key)
        keep = set()
        for tkey in target_keys:
            keep.add(tkey)
            keep |= self._downstream_closure(graph, tkey)

        for key in bypass - keep:
            bypassed = graph.nodes.get(key)
            if bypassed is not None and bypassed.status == NodeStatus.PENDING:
                await self._set_node(instance, bypassed, NodeStatus.SKIPPED_BY_GOTO)

    @staticmethod
    def _downstream_closure(graph: ExecutionGraph, start_key: str) -> set[str]:
        seen: set[str] = set()
        stack = [start_key]
        while stack:
            current = stack.pop()
            for edge in graph.edges:
                if edge.from_key == current and edge.edge_kind == EdgeKind.DEPENDENCY and edge.to_key not in seen:
                    seen.add(edge.to_key)
                    stack.append(edge.to_key)
        return seen

    # --------------------------------------------------------------- dispatch

    async def _dispatch(self, instance: WorkflowInstance, node: ExecutionNode) -> None:
        node.status = NodeStateMachine.transition(node.status, NodeStatus.RUNNABLE)
        node.status = NodeStateMachine.transition(node.status, NodeStatus.ENQUEUED)
        node.attempt = node.attempt + 1 if node.attempt else 1
        if node.step_run_id is None:
            node.step_run_id = new_step_run_id()
        if node.enqueued_at is None:
            node.enqueued_at = _now()
        node.delivery_id = new_delivery_id()

        data = self._resolve_node_data(await self.store.load_graph(instance.run_id), node)
        envelope = InvokeEnvelope(
            definition_key=instance.definition_key,
            run_id=instance.run_id,
            node_key=node.node_key,
            step_run_id=node.step_run_id,
            attempt=node.attempt,
            transport_mode=node.transport_mode,
            data=data,
        )
        outbox = OutboxRecord(
            run_id=instance.run_id,
            subject=node.target,
            payload=envelope.model_dump(mode="json"),
            delivery_id=node.delivery_id,
            kind="invoke",
            node_key=node.node_key,
        )
        await self.store.enqueue_dispatch(instance.run_id, node, outbox)

    def _resolve_node_data(self, graph: ExecutionGraph, node: ExecutionNode) -> dict[str, Any]:
        global_results = self._results_by_step(graph)
        data: dict[str, Any] = dict(node.data)
        if node.middlewares and node.middlewares.before:
            data = MiddlewareExecutor(node.middlewares.before).run(global_data=global_results, data=data)
        return Dependency.resolve_dict(data=data, local_results={}, global_results=global_results)

    # ------------------------------------------------------------ dispatch ack

    async def mark_dispatched(self, run_id: str, node_key: str, *, ack: bool) -> None:
        """
        Зафиксировать итог публикации outbox-вызова (вызывает OutboxPublisher).

        ack ok → WAITING_RESULT (или COMPLETED для fire_and_forget); ошибка → DISPATCH_ERROR
        с повтором или terminal DISPATCH_FAILED (см. SEMANTICS §4).
        """
        instance = await self.store.load_instance(run_id)
        graph = await self.store.load_graph(run_id)
        if instance is None or node_key not in graph.nodes:
            return
        node = graph.nodes[node_key]
        if node.status != NodeStatus.ENQUEUED:
            return

        node.status = NodeStateMachine.transition(node.status, NodeStatus.DISPATCHED)

        if ack:
            if node.transport_mode == TransportMode.FIRE_AND_FORGET:
                node.result = {"status": "dispatched"}
                await self._set_node(instance, node, NodeStatus.COMPLETED)
                await self.schedule(instance, graph)
            else:
                node.deadline_at = self._compute_deadline(node)
                await self._set_node(instance, node, NodeStatus.WAITING_RESULT)
            return

        await self._handle_dispatch_failure(instance, graph, node)

    async def _handle_dispatch_failure(
        self, instance: WorkflowInstance, graph: ExecutionGraph, node: ExecutionNode,
    ) -> None:
        node.status = NodeStateMachine.transition(node.status, NodeStatus.DISPATCH_ERROR)
        await self.store.persist_node(instance.run_id, node)

        if node.attempt < node.retry_policy.max_attempts and not self._is_expired(node):
            node.status = NodeStateMachine.transition(node.status, NodeStatus.ENQUEUED)
            node.attempt += 1
            node.delivery_id = new_delivery_id()
            outbox = self._invoke_outbox(instance, graph, node)
            await self.store.enqueue_dispatch(instance.run_id, node, outbox)
            return

        node.status = NodeStateMachine.transition(node.status, NodeStatus.DISPATCH_FAILED)
        await self.store.persist_node(instance.run_id, node)
        await self._after_node_failure(instance, graph, node)

    def _invoke_outbox(self, instance: WorkflowInstance, graph: ExecutionGraph, node: ExecutionNode) -> OutboxRecord:
        envelope = InvokeEnvelope(
            definition_key=instance.definition_key,
            run_id=instance.run_id,
            node_key=node.node_key,
            step_run_id=node.step_run_id,
            attempt=node.attempt,
            transport_mode=node.transport_mode,
            data=self._resolve_node_data(graph, node),
        )
        return OutboxRecord(
            run_id=instance.run_id,
            subject=node.target,
            payload=envelope.model_dump(mode="json"),
            delivery_id=node.delivery_id,
            kind="invoke",
            node_key=node.node_key,
        )

    # ----------------------------------------------------------------- result

    async def on_result(self, raw: dict[str, Any]) -> None:
        """
        Обработать orchestrator.results: версия → inbox-dedup → late/stale → применить
        результат, продвинуть downstream и финализировать run при необходимости.
        """
        message_version = raw.get("message_version")
        if not is_supported_message_version(str(message_version)):
            await self._dead_letter(raw.get("run_id"), raw.get("node_key"), "unsupported_message_version", raw)
            return

        message = StepResultMessage.model_validate(raw)
        outcome = await self.store.register_inbox(message.step_run_id, message.attempt, message_id=message.run_id)
        if outcome == "duplicate":
            return

        instance = await self.store.load_instance(message.run_id)
        graph = await self.store.load_graph(message.run_id) if instance else None
        if instance is None or graph is None or message.node_key not in graph.nodes:
            await self._dead_letter(message.run_id, message.node_key, "unknown_correlation", raw)
            return

        node = graph.nodes[message.node_key]

        if node.step_run_id != message.step_run_id:
            reason = "late_result" if is_closed(node.status) else "unexpected_state"
            await self._dead_letter(message.run_id, message.node_key, reason, raw)
            return

        if message.attempt < node.attempt:
            await self._dead_letter(message.run_id, message.node_key, "stale_attempt", raw)
            return

        # Результат сервиса может опередить mark_dispatched от OutboxPublisher: узел ещё
        # ENQUEUED/DISPATCHED. Доводим FSM до WAITING_RESULT и принимаем результат.
        if node.status in (NodeStatus.ENQUEUED, NodeStatus.DISPATCHED):
            await self._fast_forward_to_waiting(instance, node)
        elif node.status != NodeStatus.WAITING_RESULT:
            reason = "late_result" if is_closed(node.status) else "unexpected_state"
            await self._dead_letter(message.run_id, message.node_key, reason, raw)
            return

        await self._apply_result(instance, graph, node, message)

    async def _fast_forward_to_waiting(self, instance: WorkflowInstance, node: ExecutionNode) -> None:
        if node.status == NodeStatus.ENQUEUED:
            node.status = NodeStateMachine.transition(node.status, NodeStatus.DISPATCHED)
        node.status = NodeStateMachine.transition(node.status, NodeStatus.WAITING_RESULT)
        if node.deadline_at is None:
            node.deadline_at = self._compute_deadline(node)
        await self.store.persist_node(instance.run_id, node)

    async def _apply_result(
        self,
        instance: WorkflowInstance,
        graph: ExecutionGraph,
        node: ExecutionNode,
        message: StepResultMessage,
    ) -> None:
        if message.error is not None and message.error.failure_class == FailureClass.BUSINESS:
            await self._after_node_failure(instance, graph, node, base_status=NodeStatus.FAILED)
            return

        if message.error is not None:
            await self._retry_or_fail_result(instance, graph, node)
            return

        result = message.result or {}
        if node.middlewares and node.middlewares.after:
            result = MiddlewareExecutor(node.middlewares.after).run(
                global_data=self._results_by_step(graph), data=result,
            )
        node.result = result
        await self._set_node(instance, node, NodeStatus.COMPLETED)
        await self.schedule(instance, graph)

    async def _retry_or_fail_result(
        self, instance: WorkflowInstance, graph: ExecutionGraph, node: ExecutionNode,
    ) -> None:
        if node.attempt < node.retry_policy.max_attempts and not self._is_expired(node):
            node.status = NodeStateMachine.transition(node.status, NodeStatus.ENQUEUED)
            node.attempt += 1
            node.step_run_id = new_step_run_id()
            node.delivery_id = new_delivery_id()
            outbox = self._invoke_outbox(instance, graph, node)
            await self.store.enqueue_dispatch(instance.run_id, node, outbox)
            return
        await self._after_node_failure(instance, graph, node, base_status=NodeStatus.FAILED)

    async def cancel(self, run_id: str, *, reason: str | None = None) -> bool:
        """
        Отменить run: незапущенные узлы → CANCELLED, ожидающие → cooperative cancel.

        Возвращает False, если run не найден или уже в terminal-состоянии.
        """
        instance = await self.store.load_instance(run_id)
        if instance is None or instance.status in (
            InstanceStatus.COMPLETED,
            InstanceStatus.FAILED,
            InstanceStatus.CANCELLED,
        ):
            return False
        graph = await self.store.load_graph(run_id)
        instance.status = InstanceStatus.CANCELLING
        await self.store.persist_instance(instance)

        for node in graph.nodes.values():
            if node.status in (NodeStatus.PENDING, NodeStatus.RUNNABLE, NodeStatus.ENQUEUED):
                await self._set_node(instance, node, NodeStatus.CANCELLED)
            elif node.status == NodeStatus.WAITING_RESULT:
                await self._set_node(instance, node, NodeStatus.CANCELLING)
                await self._set_node(instance, node, NodeStatus.CANCELLED)

        instance.status = InstanceStatus.CANCELLED
        instance.failure_reason = reason or "cancelled"
        await self.store.persist_instance(instance)
        await self._publish_completion(instance)
        return True

    async def expire_node(self, run_id: str, node_key: str) -> None:
        """Перевести просроченный узел в EXPIRED и применить on_failure (TimeoutWatcher)."""
        instance = await self.store.load_instance(run_id)
        graph = await self.store.load_graph(run_id)
        if instance is None or node_key not in graph.nodes:
            return
        node = graph.nodes[node_key]
        if node.status not in (NodeStatus.ENQUEUED, NodeStatus.DISPATCHED, NodeStatus.WAITING_RESULT):
            return
        await self._set_node(instance, node, NodeStatus.EXPIRED)
        await self._after_node_failure(instance, graph, node)

    # ---------------------------------------------------------------- failures

    async def _after_node_failure(
        self,
        instance: WorkflowInstance,
        graph: ExecutionGraph,
        node: ExecutionNode,
        *,
        base_status: NodeStatus | None = None,
    ) -> None:
        if node.on_failure == OnFailure.FAIL:
            if base_status is not None and node.status != base_status:
                await self._set_node(instance, node, base_status)
            await self._fail_instance(instance, graph, node, reason="node_failed")
            return

        target = {
            OnFailure.SKIP: NodeStatus.SKIPPED,
            OnFailure.ABANDON: NodeStatus.ABANDONED,
        }[node.on_failure]
        if node.status not in (NodeStatus.DISPATCH_FAILED, NodeStatus.EXPIRED):
            await self._set_node(instance, node, target)
        if node.on_failure == OnFailure.ABANDON:
            await logger.awarning(
                "node abandoned",
                run_id=instance.run_id,
                node_key=node.node_key,
                definition_key=instance.definition_key,
            )
        await self.schedule(instance, graph)

    async def _fail_instance(
        self, instance: WorkflowInstance, graph: ExecutionGraph, node: ExecutionNode, *, reason: str,
    ) -> None:
        completed = [n for n in graph.nodes.values() if n.status == NodeStatus.COMPLETED]
        outcome = await self.compensation.on_node_failed(
            CompensationContext(instance=instance, failed_node=node, completed_nodes=completed),
        )
        instance.status = outcome.instance_status
        instance.failure_reason = reason
        await self.store.persist_instance(instance)
        await self._publish_completion(instance)

    # --------------------------------------------------------------- finalize

    async def _maybe_finalize(self, instance: WorkflowInstance, graph: ExecutionGraph) -> None:
        if instance.status in (InstanceStatus.FAILED, InstanceStatus.COMPLETED):
            return
        if all(is_closed(node.status) for node in graph.nodes.values()):
            await self._complete_instance(instance, graph)

    async def _complete_instance(self, instance: WorkflowInstance, graph: ExecutionGraph) -> None:
        if instance.status in (InstanceStatus.FAILED, InstanceStatus.COMPLETED):
            return
        outputs, missing = self._resolve_outputs(instance, graph)
        if missing:
            instance.failure_reason = f"OUTPUT_MISSING:{','.join(missing)}"
            instance.status = InstanceStatus.FAILED
            await self.store.persist_instance(instance)
            await self._publish_completion(instance)
            return
        instance.outputs = outputs
        instance.status = InstanceStatus.COMPLETED
        await self.store.persist_instance(instance)
        await self._publish_completion(instance)

    def _resolve_outputs(
        self, instance: WorkflowInstance, graph: ExecutionGraph,
    ) -> tuple[dict[str, Any], list[str]]:
        global_results = self._results_by_step(graph)
        outputs: dict[str, Any] = {}
        missing: list[str] = []
        for name, spec in instance.outputs_spec.items():
            dep = Dependency.parse(spec.ref)
            try:
                value = dep.resolve(global_results[dep.step_id]) if dep and dep.step_id in global_results else None
                found = dep is not None and dep.step_id in global_results
            except Exception:
                value, found = None, False
            if found:
                outputs[name] = value
            elif spec.required:
                missing.append(name)
            else:
                outputs[name] = spec.default
        return outputs, missing

    async def _publish_completion(self, instance: WorkflowInstance) -> None:
        if instance.completion_published_at is not None:
            return
        message = WorkflowCompletedMessage(
            definition_key=instance.definition_key,
            run_id=instance.run_id,
            status=instance.status.value,
            outputs=instance.outputs,
            failure_reason=instance.failure_reason,
        )
        outbox = OutboxRecord(
            run_id=instance.run_id,
            subject=self.completed_subject,
            payload=message.model_dump(mode="json"),
            delivery_id=new_delivery_id(),
            kind="workflow_completed",
        )
        await self.store.enqueue_outbox(outbox)

    # --------------------------------------------------------------- helpers

    @staticmethod
    def _results_by_step(graph: ExecutionGraph) -> dict[int, Any]:
        return {node.step_id: node.result for node in graph.nodes.values() if node.result is not None}

    @staticmethod
    def _compute_deadline(node: ExecutionNode) -> datetime:
        result_deadline = _now() + timedelta(seconds=node.retry_policy.result_wait_timeout_sec)
        if node.valid_for_sec is not None and node.enqueued_at is not None:
            ttl_deadline = node.enqueued_at + timedelta(seconds=node.valid_for_sec)
            return min(result_deadline, ttl_deadline)
        return result_deadline

    @staticmethod
    def _is_expired(node: ExecutionNode) -> bool:
        if node.valid_for_sec is None or node.enqueued_at is None:
            return False
        return _now() > node.enqueued_at + timedelta(seconds=node.valid_for_sec)

    async def _set_node(self, instance: WorkflowInstance, node: ExecutionNode, target: NodeStatus) -> None:
        node.status = NodeStateMachine.transition(node.status, target)
        await self.store.persist_node(instance.run_id, node)

    async def _dead_letter(
        self, run_id: str | None, node_key: str | None, reason: str, payload: dict[str, Any],
    ) -> None:
        await self.store.dead_letter(
            DeadLetterRecord(subject="orchestrator.results", reason=reason, payload=payload, run_id=run_id, node_key=node_key),
        )
        await logger.awarning("dead letter", reason=reason, run_id=run_id, node_key=node_key)
