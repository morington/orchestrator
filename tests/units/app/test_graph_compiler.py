from orchestrator.app.application.compiler import GraphCompiler
from orchestrator.core.entities import WorkflowDefinition
from orchestrator.core.enums import EdgeKind
from orchestrator.core.statuses import NodeStatus


def _definition() -> WorkflowDefinition:
    return WorkflowDefinition.model_validate(
        {
            "definition_key": "demo",
            "idempotency_key": "i",
            "steps": [
                {
                    "step_id": 1,
                    "meta": {"target": "service.a"},
                    "data": {},
                    "filters": [{"filter_id": 1, "condition": "true", "then": "goto", "targets": [2]}],
                },
                {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
            ],
        },
    )


def test_compile_builds_nodes_and_edges():
    _instance, graph = GraphCompiler().compile(_definition())
    assert set(graph.nodes) == {"step-1", "step-2"}
    assert all(node.status == NodeStatus.PENDING for node in graph.nodes.values())

    dependency_edges = [e for e in graph.edges if e.edge_kind == EdgeKind.DEPENDENCY]
    assert len(dependency_edges) == 1
    assert dependency_edges[0].from_key == "step-1"
    assert dependency_edges[0].to_key == "step-2"


def test_goto_edges_compiled_inactive():
    _, graph = GraphCompiler().compile(_definition())
    goto_edges = [e for e in graph.edges if e.edge_kind == EdgeKind.GOTO]
    assert len(goto_edges) == 1
    assert goto_edges[0].active is False


def test_definition_hash_is_stable():
    instance_a, _ = GraphCompiler().compile(_definition())
    instance_b, _ = GraphCompiler().compile(_definition())
    assert instance_a.definition_hash == instance_b.definition_hash
    assert instance_a.run_id != instance_b.run_id
