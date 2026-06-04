"""
Mock-микросервис для локального прогона пайплайнов.

Подписывается на `service.>`, эхо-отвечает `StepResultMessage` на
`orchestrator.results`. Для `fire_and_forget` ответ не отправляется.
Запуск: `DEV=true uv run python -m mocks.service`.
"""

import asyncio
from typing import Any

from faststream import FastStream
from faststream.nats import NatsBroker
from structlog import getLogger

from orchestrator.app.configuration.config import Configuration

logger = getLogger("MOCK")

config = Configuration()
broker = NatsBroker(config.nats.servers, logger=None)
app = FastStream(broker)


def _build_result(envelope: dict[str, Any]) -> dict[str, Any]:
    data = envelope.get("data", {})
    return {
        "message_version": envelope.get("message_version", "1.0"),
        "definition_key": envelope["definition_key"],
        "run_id": envelope["run_id"],
        "node_key": envelope["node_key"],
        "step_run_id": envelope["step_run_id"],
        "attempt": envelope["attempt"],
        "result": {"echo": data, "text": data.get("text", envelope["node_key"]), "status": "ok"},
    }


@broker.subscriber("service.>")
async def handle_invoke(envelope: dict[str, Any]) -> None:
    await logger.ainfo("mock invoke", node_key=envelope.get("node_key"), run_id=envelope.get("run_id"))
    if envelope.get("transport_mode") == "fire_and_forget":
        return
    await broker.publish(_build_result(envelope), config.subjects.results)


if __name__ == "__main__":
    asyncio.run(app.run())
