import asyncio
import os
import sys
import uuid
from typing import Any

from faststream.nats import NatsBroker

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
START_SUBJECT = "orchestrator.workflow.start"
COMPLETED_SUBJECT = "orchestrator.workflow.completed"


def build_pipeline(expression: str) -> dict[str, Any]:
    """Описать пайплайн: посчитать выражение, затем собрать текст ответа."""
    return {
        "message_version": "1.0",
        "pipeline_version": "1.0",
        "definition_key": "calculator",
        "idempotency_key": str(uuid.uuid4()),
        "business_ref": expression,
        "outputs": {"answer": {"ref": "$2:text", "required": True}},
        "steps": [
            {
                "step_id": 1,
                "meta": {"target": "service.calc"},
                "data": {"expression": expression},
            },
            {
                "step_id": 2,
                "meta": {"target": "service.assemble"},
                "depends_on": [{"node": 1, "policy": "requires_success"}],
                "data": {"expression": expression, "value": "$1:value"},
            },
        ],
    }


async def main(expressions: list[str]) -> None:
    broker = NatsBroker(NATS_URL)
    expected = len(expressions)
    received = 0
    finished = asyncio.Event()

    @broker.subscriber(COMPLETED_SUBJECT)
    async def on_completed(message: dict[str, Any]) -> None:
        nonlocal received
        answer = message.get("outputs", {}).get("answer")
        if answer is not None:
            print(answer)
        else:
            print(f"[{message.get('status')}] {message.get('failure_reason')}")
        received += 1
        if received >= expected:
            finished.set()

    await broker.connect()
    await broker.start()

    for expression in expressions:
        await broker.publish(build_pipeline(expression), START_SUBJECT)
    print(f"Отправлено примеров: {expected}. Ждём ответы...\n")

    try:
        await asyncio.wait_for(finished.wait(), timeout=30.0)
    except TimeoutError:
        print(f"\nПолучено {received} из {expected} (таймаут).")

    await broker.stop()


if __name__ == "__main__":
    args = sys.argv[1:] or ["2+2", "6-9*8", "10/4", "(3+5)*2"]
    asyncio.run(main(args))
