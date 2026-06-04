from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from orchestrator.core.entities import (
    DependsOnSpec,
    FilterEntity,
    MiddlewareEntity,
    RetryPolicy,
)
from orchestrator.core.enums import EdgeKind, NodeType, OnFailure, TransportMode
from orchestrator.core.statuses import NodeStatus


def node_key_for(step_id: int) -> str:
    """Каноническое имя узла графа из DSL step_id."""
    return f"step-{step_id}"


@dataclass
class ExecutionNode:
    """
    Узел графа исполнения — runtime-состояние одного шага.

    Граф для существующего run_id заморожен: структурные поля не меняются,
    меняются только status/attempt/step_run_id/таймстемпы и lease.
    """

    node_key: str
    step_id: int
    node_type: NodeType
    target: str
    transport_mode: TransportMode
    on_failure: OnFailure
    retry_policy: RetryPolicy
    data: dict[str, Any]
    valid_for_sec: int | None = None
    depends_on: list[DependsOnSpec] = field(default_factory=list)
    filters: list[FilterEntity] = field(default_factory=list)
    middlewares: MiddlewareEntity | None = None

    status: NodeStatus = NodeStatus.PENDING
    attempt: int = 0
    step_run_id: str | None = None
    delivery_id: str | None = None
    result: dict[str, Any] | None = None
    enqueued_at: datetime | None = None
    deadline_at: datetime | None = None

    locked_by: str | None = None
    locked_until: datetime | None = None
    revision: int = 0


@dataclass
class ExecutionEdge:
    """
    Ребро графа исполнения.

    GOTO-рёбра по умолчанию неактивны (`active=False`) и активируются при
    срабатывании GOTO-фильтра.
    """

    from_key: str
    to_key: str
    edge_kind: EdgeKind = EdgeKind.DEPENDENCY
    active: bool = True


@dataclass
class ExecutionGraph:
    """
    In-memory представление графа run.

    Источник правды для scheduler/runtime — узлы и рёбра (не snapshot definition).
    """

    nodes: dict[str, ExecutionNode] = field(default_factory=dict)
    edges: list[ExecutionEdge] = field(default_factory=list)

    def add_node(self, node: ExecutionNode) -> None:
        self.nodes[node.node_key] = node

    def add_edge(self, edge: ExecutionEdge) -> None:
        self.edges.append(edge)

    def get_node(self, node_key: str) -> ExecutionNode:
        return self.nodes[node_key]

    def by_step_id(self, step_id: int) -> ExecutionNode | None:
        return self.nodes.get(node_key_for(step_id))

    def dependency_edges_into(self, node_key: str) -> list[ExecutionEdge]:
        """Активные рёбра-зависимости, входящие в узел."""
        return [
            e
            for e in self.edges
            if e.to_key == node_key and e.edge_kind == EdgeKind.DEPENDENCY and e.active
        ]

    def downstream_of(self, node_key: str) -> list[ExecutionNode]:
        """Узлы, для которых node_key является upstream по dependency-рёбрам."""
        keys = {e.to_key for e in self.edges if e.from_key == node_key and e.edge_kind == EdgeKind.DEPENDENCY}
        return [self.nodes[k] for k in keys if k in self.nodes]

    def goto_edges_from(self, node_key: str) -> list[ExecutionEdge]:
        return [e for e in self.edges if e.from_key == node_key and e.edge_kind == EdgeKind.GOTO]
