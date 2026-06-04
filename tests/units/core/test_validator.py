import pytest

from orchestrator.core.entities import WorkflowDefinition
from orchestrator.core.errors import WorkflowDefinitionError
from orchestrator.core.validator import WorkflowDefinitionValidator


def _validate(payload: dict) -> None:
    WorkflowDefinitionValidator().validate(WorkflowDefinition.model_validate(payload))


def test_valid_linear_definition():
    _validate(
        {
            "definition_key": "k",
            "idempotency_key": "i",
            "outputs": {"f": "$2:result"},
            "steps": [
                {"step_id": 1, "meta": {"target": "service.a"}, "data": {}},
                {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {"x": "$1:result"}},
            ],
        },
    )


def test_dangling_dependency_rejected():
    with pytest.raises(WorkflowDefinitionError, match="unknown step 9"):
        _validate(
            {
                "definition_key": "k",
                "idempotency_key": "i",
                "steps": [{"step_id": 1, "meta": {"target": "service.a"}, "depends_on": [9], "data": {}}],
            },
        )


def test_cycle_rejected():
    with pytest.raises(WorkflowDefinitionError, match="cycle"):
        _validate(
            {
                "definition_key": "k",
                "idempotency_key": "i",
                "steps": [
                    {"step_id": 1, "meta": {"target": "service.a"}, "depends_on": [2], "data": {}},
                    {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
                ],
            },
        )


def test_goto_unknown_target_rejected():
    with pytest.raises(WorkflowDefinitionError, match="GOTO to unknown step 5"):
        _validate(
            {
                "definition_key": "k",
                "idempotency_key": "i",
                "steps": [
                    {
                        "step_id": 1,
                        "meta": {"target": "service.a"},
                        "data": {},
                        "filters": [{"filter_id": 1, "condition": "true", "then": "goto", "targets": [5]}],
                    },
                ],
            },
        )


def test_unknown_data_ref_rejected():
    with pytest.raises(WorkflowDefinitionError, match="data references unknown step 7"):
        _validate(
            {
                "definition_key": "k",
                "idempotency_key": "i",
                "steps": [{"step_id": 1, "meta": {"target": "service.a"}, "data": {"x": "$7:result"}}],
            },
        )


def test_unknown_output_ref_rejected():
    with pytest.raises(WorkflowDefinitionError, match="references unknown step 8"):
        _validate(
            {
                "definition_key": "k",
                "idempotency_key": "i",
                "outputs": {"f": "$8:result"},
                "steps": [{"step_id": 1, "meta": {"target": "service.a"}, "data": {}}],
            },
        )


def test_unsupported_pipeline_version_rejected():
    with pytest.raises(WorkflowDefinitionError, match="unsupported pipeline_version"):
        _validate(
            {
                "definition_key": "k",
                "idempotency_key": "i",
                "pipeline_version": "2.0",
                "steps": [{"step_id": 1, "meta": {"target": "service.a"}, "data": {}}],
            },
        )
