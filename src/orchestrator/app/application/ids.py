import hashlib
import json
import time
import uuid
from typing import Any


def _ts_prefix() -> str:
    """Лексикографически сортируемый префикс по времени (мс)."""
    return format(int(time.time() * 1000), "012x")


def new_run_id() -> str:
    """Сгенерировать уникальный run_id (сортируемый по времени)."""
    return f"run_{_ts_prefix()}{uuid.uuid4().hex[:12]}"


def new_step_run_id() -> str:
    """Сгенерировать step_run_id (стабильный на весь логический шаг)."""
    return f"op_{_ts_prefix()}{uuid.uuid4().hex[:12]}"


def new_delivery_id() -> str:
    """Сгенерировать delivery_id (на одну outbox-запись → Nats-Msg-Id)."""
    return f"dlv_{uuid.uuid4().hex}"


def definition_hash(snapshot: dict[str, Any]) -> str:
    """SHA256 от канонического JSON определения (anti-drift)."""
    canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
