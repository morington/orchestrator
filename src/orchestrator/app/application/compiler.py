from orchestrator.app.application.ids import definition_hash, new_run_id
from orchestrator.app.domain.graph import (
    ExecutionEdge,
    ExecutionGraph,
    ExecutionNode,
    node_key_for,
)
from orchestrator.app.domain.instance import WorkflowInstance
from orchestrator.core.entities import GotoFilterAction, WorkflowDefinition
from orchestrator.core.enums import EdgeKind, NodeType
from orchestrator.core.statuses import InstanceStatus, NodeStatus


class GraphCompiler:
    """
    Компилирует валидное определение workflow в замороженный граф исполнения.

    `steps` + `depends_on` → узлы task + dependency-рёбра; GOTO-фильтры → неактивные
    GOTO-рёбра (активируются в runtime). Граф для нового run неизменен после создания.
    """

    def compile(self, definition: WorkflowDefinition) -> tuple[WorkflowInstance, ExecutionGraph]:
        """
        Скомпилировать определение в (instance, graph).

        Args:
            definition: Уже провалидированное определение.

        Returns:
            Кортеж нового WorkflowInstance (status PENDING) и ExecutionGraph.
        """
        snapshot = definition.model_dump(mode="json")
        graph = self._build_graph(definition)

        instance = WorkflowInstance(
            run_id=new_run_id(),
            definition_key=definition.definition_key,
            idempotency_key=definition.idempotency_key,
            business_ref=definition.business_ref,
            definition_hash=definition_hash(snapshot),
            pipeline_version=definition.pipeline_version,
            outputs_spec=dict(definition.outputs),
            definition_snapshot=snapshot,
            status=InstanceStatus.PENDING,
        )
        return instance, graph

    @staticmethod
    def _build_graph(definition: WorkflowDefinition) -> ExecutionGraph:
        graph = ExecutionGraph()
        for step in definition.steps:
            graph.add_node(
                ExecutionNode(
                    node_key=node_key_for(step.step_id),
                    step_id=step.step_id,
                    node_type=NodeType.TASK,
                    target=step.meta.target,
                    transport_mode=step.meta.transport_mode,
                    on_failure=step.meta.on_failure,
                    retry_policy=step.meta.retry_policy,
                    data=step.data,
                    valid_for_sec=step.meta.valid_for_sec,
                    depends_on=list(step.depends_on or []),
                    filters=list(step.filters or []),
                    middlewares=step.middlewares,
                    status=NodeStatus.PENDING,
                ),
            )

        for step in definition.steps:
            to_key = node_key_for(step.step_id)
            for dep in step.depends_on or []:
                graph.add_edge(ExecutionEdge(from_key=node_key_for(dep.node), to_key=to_key, edge_kind=EdgeKind.DEPENDENCY))
            for flt in step.filters or []:
                if isinstance(flt, GotoFilterAction):
                    for target in flt.targets:
                        graph.add_edge(
                            ExecutionEdge(
                                from_key=node_key_for(step.step_id),
                                to_key=node_key_for(target),
                                edge_kind=EdgeKind.GOTO,
                                active=False,
                            ),
                        )
        return graph
