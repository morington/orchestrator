from typing import Any

import ormsgpack


def decode_body(body: dict[str, Any] | bytes | bytearray, *, development: bool) -> dict[str, Any]:
    """
    Десериализовать тело сообщения.

    В dev — FastStream уже отдаёт dict (JSON), в prod — MessagePack bytes.
    """
    if isinstance(body, dict):
        return body
    if not isinstance(body, (bytes, bytearray)):
        raise TypeError(f"Unsupported body type: {type(body)}")
    if development:
        import json

        return json.loads(bytes(body).decode())
    return ormsgpack.unpackb(bytes(body))


def encode_body(payload: dict[str, Any], *, development: bool) -> dict[str, Any] | bytes:
    """Сериализовать тело: dict (JSON) в dev, MessagePack в prod."""
    return payload if development else ormsgpack.packb(payload)
