from orchestrator.app.domain.messages import (
    InvokeEnvelope,
    StepResultMessage,
    WorkflowCompletedMessage,
)
from orchestrator.core.enums import FailureClass, TransportMode


def test_invoke_envelope_round_trip():
    env = InvokeEnvelope(
        definition_key="demo",
        run_id="run_1",
        node_key="step-1",
        step_run_id="op_1",
        attempt=1,
        data={"x": 1},
    )
    dumped = env.model_dump(mode="json")
    assert dumped["message_version"] == "1.0"
    assert dumped["transport_mode"] == TransportMode.ASYNC_RESULT_SUBJECT
    assert InvokeEnvelope.model_validate(dumped) == env


def test_step_result_with_error():
    msg = StepResultMessage.model_validate(
        {
            "message_version": "1.0",
            "definition_key": "demo",
            "run_id": "run_1",
            "node_key": "step-1",
            "step_run_id": "op_1",
            "attempt": 2,
            "error": {"failure_class": "transient", "message": "retry me"},
        },
    )
    assert msg.error.failure_class == FailureClass.TRANSIENT
    assert msg.result is None


def test_completed_message_defaults():
    msg = WorkflowCompletedMessage(definition_key="demo", run_id="run_1", status="completed")
    assert msg.message_version == "1.0"
    assert msg.outputs == {}
