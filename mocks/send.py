"""
Отправить декларацию из файла на `orchestrator.workflow.start`.

Запуск: `DEV=true uv run python -m mocks.send examples/pipeline_linear.json`.
"""

import asyncio
import json
import sys
from pathlib import Path

from faststream.nats import NatsBroker

from orchestrator.app.configuration.config import Configuration


async def _send(payload: dict) -> None:
    config = Configuration()
    payload.setdefault("message_version", "1.0")
    broker = NatsBroker(config.nats.servers, logger=None)
    await broker.connect()
    try:
        response = await broker.request(payload, config.subjects.workflow_start, timeout=10.0)
        print(await response.decode())
    finally:
        await broker.stop()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m mocks.send <definition.json>")
    payload = json.loads(Path(sys.argv[1]).read_text())
    asyncio.run(_send(payload))


if __name__ == "__main__":
    main()
