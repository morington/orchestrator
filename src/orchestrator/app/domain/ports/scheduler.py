from typing import Protocol

from orchestrator.app.domain.graph import ExecutionGraph, ExecutionNode


class GraphScheduler(Protocol):
    """Порт планировщика: какие узлы готовы к запуску по состоянию графа."""

    def list_runnable(self, graph: ExecutionGraph) -> list[ExecutionNode]:
        """Вернуть PENDING-узлы, все зависимости которых удовлетворены."""
        ...
