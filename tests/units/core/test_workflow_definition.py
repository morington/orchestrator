import pytest
from pydantic import ValidationError

from orchestrator.core.entities import DependencyPolicy, WorkflowDefinition


def _base_steps() -> list[dict]:
    return [
        {"step_id": 1, "meta": {"target": "service.a"}, "data": {}},
        {"step_id": 2, "meta": {"target": "service.b"}, "depends_on": [1], "data": {}},
    ]


def test_minimal_definition_parses():
    definition = WorkflowDefinition.model_validate(
        {"definition_key": "k", "idempotency_key": "i", "steps": _base_steps()},
    )
    assert definition.pipeline_version == "1.0"
    assert definition.steps[1].depends_on[0].node == 1
    assert definition.steps[1].depends_on[0].policy == DependencyPolicy.REQUIRES_SUCCESS


def test_depends_on_object_form():
    steps = _base_steps()
    steps[1]["depends_on"] = [{"node": 1, "policy": "requires_closed"}]
    definition = WorkflowDefinition.model_validate(
        {"definition_key": "k", "idempotency_key": "i", "steps": steps},
    )
    assert definition.steps[1].depends_on[0].policy == DependencyPolicy.REQUIRES_CLOSED


def test_outputs_string_shorthand_is_required_ref():
    definition = WorkflowDefinition.model_validate(
        {"definition_key": "k", "idempotency_key": "i", "outputs": {"f": "$2:result"}, "steps": _base_steps()},
    )
    assert definition.outputs["f"].ref == "$2:result"
    assert definition.outputs["f"].required is True


def test_outputs_non_reference_rejected():
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(
            {"definition_key": "k", "idempotency_key": "i", "outputs": {"f": "literal"}, "steps": _base_steps()},
        )


def test_pipeline_id_field_is_rejected():
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(
            {"definition_key": "k", "idempotency_key": "i", "pipeline_id": 1, "steps": _base_steps()},
        )


def test_compensation_field_is_rejected():
    steps = _base_steps()
    steps[0]["compensation"] = {"target": "service.refund"}
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(
            {"definition_key": "k", "idempotency_key": "i", "steps": steps},
        )


def test_empty_steps_rejected():
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate({"definition_key": "k", "idempotency_key": "i", "steps": []})
