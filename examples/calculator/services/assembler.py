from typing import Any

from common import NATS_URL, RESULTS_SUBJECT, build_result
from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker(NATS_URL)
app = FastStream(broker)


def format_value(value: float) -> str:
    """Показать целое без дробной части, дробное — как есть."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


@broker.subscriber("service.assemble")
async def assemble(envelope: dict[str, Any]) -> None:
    data = envelope["data"]
    text = f"Ответ на пример ({data['expression']}): {format_value(data['value'])}"
    await broker.publish(build_result(envelope, {"text": text}), RESULTS_SUBJECT)


if __name__ == "__main__":
    import asyncio

    asyncio.run(app.run())
