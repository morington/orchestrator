from orchestrator.app.domain.graph import ExecutionGraph, ExecutionNode
from orchestrator.core.enums import DependencyPolicy
from orchestrator.core.statuses import NodeStatus, is_closed, is_success


class DagGraphScheduler:
    """
    Планировщик DAG task-узлов.

    runnable = PENDING-узлы, все активные зависимости которых удовлетворены по своей
    политике (см. docs/SEMANTICS.md §2). join_states/fork — задел, в 1.0 не используются.
    """

    def list_runnable(self, graph: ExecutionGraph) -> list[ExecutionNode]:
        """Вернуть готовые к запуску PENDING-узлы."""
        return [
            node
            for node in graph.nodes.values()
            if node.status == NodeStatus.PENDING and self._dependencies_satisfied(graph, node)
        ]

    def list_unreachable(self, graph: ExecutionGraph) -> list[ExecutionNode]:
        """
        Вернуть PENDING-узлы, которые уже не смогут запуститься.

        Это узлы с зависимостью requires_success на upstream, закрытый без success.
        """
        return [
            node
            for node in graph.nodes.values()
            if node.status == NodeStatus.PENDING and self._permanently_blocked(graph, node)
        ]

    @staticmethod
    def _policy_for(node: ExecutionNode, upstream_step_id: int) -> DependencyPolicy:
        for dep in node.depends_on:
            if dep.node == upstream_step_id:
                return dep.policy
        return DependencyPolicy.REQUIRES_SUCCESS

    def _dependencies_satisfied(self, graph: ExecutionGraph, node: ExecutionNode) -> bool:
        for edge in graph.dependency_edges_into(node.node_key):
            upstream = graph.nodes.get(edge.from_key)
            if upstream is None:
                continue
            policy = self._policy_for(node, upstream.step_id)
            if not self._edge_satisfied(upstream.status, policy):
                return False
        return True

    def _permanently_blocked(self, graph: ExecutionGraph, node: ExecutionNode) -> bool:
        for edge in graph.dependency_edges_into(node.node_key):
            upstream = graph.nodes.get(edge.from_key)
            if upstream is None:
                continue
            policy = self._policy_for(node, upstream.step_id)
            if policy == DependencyPolicy.REQUIRES_SUCCESS and is_closed(upstream.status) and not is_success(
                upstream.status,
            ):
                return True
        return False

    @staticmethod
    def _edge_satisfied(upstream_status: NodeStatus, policy: DependencyPolicy) -> bool:
        if policy == DependencyPolicy.OPTIONAL:
            return True
        if policy == DependencyPolicy.REQUIRES_CLOSED:
            return is_closed(upstream_status)
        return is_success(upstream_status)
