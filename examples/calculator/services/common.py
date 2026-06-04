import os
from typing import Any

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
RESULTS_SUBJECT = os.getenv("RESULTS_SUBJECT", "orchestrator.results")


def build_result(envelope: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    """Собрать ответ шага, повторив корреляционные поля из входящего вызова."""
    return {
        "message_version": envelope.get("message_version", "1.0"),
        "definition_key": envelope["definition_key"],
        "run_id": envelope["run_id"],
        "node_key": envelope["node_key"],
        "step_run_id": envelope["step_run_id"],
        "attempt": envelope["attempt"],
        "result": result,
    }


def build_error(envelope: dict[str, Any], message: str) -> dict[str, Any]:
    """Собрать ответ с бизнес-ошибкой шага."""
    reply = build_result(envelope, {})
    reply.pop("result")
    reply["error"] = {"failure_class": "business", "message": message}
    return reply
