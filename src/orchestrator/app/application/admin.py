from typing import Any

from orchestrator.app.application.runtime import WorkflowRuntime
from orchestrator.app.domain.contracts import RUNTIME_VERSION
from orchestrator.app.domain.graph import ExecutionGraph, ExecutionNode
from orchestrator.core.statuses import InstanceStatus, NodeStatus, is_closed


class AdminError(Exception):
    """Ошибка административной операции."""


class AdminService:
    """
    Административные операции по NATS admin-subjects (см. docs/OPERATIONS.md).

    Ответы содержат runtime_version + definition_key + run_id; узловые ID — только в logs.
    """

    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime
        self.store = runtime.store

    async def inspect(self, run_id: str) -> dict[str, Any]:
        instance = await self.store.load_instance(run_id)
        if instance is None:
            raise AdminError(f"unknown run_id {run_id}")
        graph = await self.store.load_graph(run_id)
        return {
            "runtime_version": RUNTIME_VERSION,
            "definition_key": instance.definition_key,
            "run_id": instance.run_id,
            "status": instance.status.value,
            "nodes": [{"node_key": n.node_key, "status": n.status.value, "attempt": n.attempt} for n in graph.nodes.values()],
        }

    async def retry_node(self, run_id: str, node_key: str) -> dict[str, Any]:
        instance = await self.store.load_instance(run_id)
        if instance is None:
            raise AdminError(f"unknown run_id {run_id}")
        graph = await self.store.load_graph(run_id)
        node = graph.nodes.get(node_key)
        if node is None:
            raise AdminError(f"unknown node {node_key}")
        if node.status not in (NodeStatus.FAILED, NodeStatus.ABANDONED, NodeStatus.DISPATCH_FAILED, NodeStatus.EXPIRED):
            raise AdminError(f"node {node_key} is not in a retryable terminal status")
        if not self._is_terminal_leaf(graph, node):
            raise AdminError("retry_node 1.0 поддерживает только terminal leaf node")

        node.status = NodeStatus.PENDING
        node.attempt = 0
        node.step_run_id = None
        node.delivery_id = None
        node.enqueued_at = None
        node.deadline_at = None
        node.result = None
        await self.store.persist_node(run_id, node)

        instance.status = InstanceStatus.RUNNING
        instance.failure_reason = None
        instance.completion_published_at = None
        await self.store.persist_instance(instance)
        await self.runtime.schedule(instance, graph)
        return await self.inspect(run_id)

    async def resume(self, run_id: str) -> dict[str, Any]:
        instance = await self.store.load_instance(run_id)
        if instance is None:
            raise AdminError(f"unknown run_id {run_id}")
        graph = await self.store.load_graph(run_id)
        if instance.status == InstanceStatus.PENDING:
            instance.status = InstanceStatus.RUNNING
            await self.store.persist_instance(instance)
        await self.runtime.schedule(instance, graph)
        return await self.inspect(run_id)

    async def abandon_node(self, run_id: str, node_key: str) -> dict[str, Any]:
        instance = await self.store.load_instance(run_id)
        if instance is None:
            raise AdminError(f"unknown run_id {run_id}")
        graph = await self.store.load_graph(run_id)
        node = graph.nodes.get(node_key)
        if node is None:
            raise AdminError(f"unknown node {node_key}")
        node.status = NodeStatus.ABANDONED
        await self.store.persist_node(run_id, node)
        await self.runtime.schedule(instance, graph)
        return await self.inspect(run_id)

    async def cancel(self, run_id: str, reason: str | None = None) -> dict[str, Any]:
        await self.runtime.cancel(run_id, reason=reason)
        return await self.inspect(run_id)

    @staticmethod
    def _is_terminal_leaf(graph: ExecutionGraph, node: ExecutionNode) -> bool:
        downstream = graph.downstream_of(node.node_key)
        return all(is_closed(d.status) for d in downstream)
