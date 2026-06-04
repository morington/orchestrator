from collections import Counter

import structlog

from orchestrator.app.configuration.loggers import Loggers

logger = structlog.getLogger(Loggers.engine.name)

# Разрешённые низкокардинальные метки (docs/SEMANTICS §9): без run_id / node_key / step_run_id.
ALLOWED_LABELS = frozenset(
    {"status", "node_type", "transport_mode", "target_group", "failure_class", "message_version", "kind", "table"},
)


class Metrics:
    """
    Лёгкий счётчик метрик (structlog-friendly). In-memory counters с низкой
    кардинальностью; интеграция с Prometheus — точка расширения.
    """

    def __init__(self) -> None:
        self._counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()

    def incr(self, name: str, *, value: int = 1, **labels: str) -> None:
        safe = {k: v for k, v in labels.items() if k in ALLOWED_LABELS}
        self._counters[(name, tuple(sorted(safe.items())))] += value

    def snapshot(self) -> dict[str, int]:
        return {
            name if not labels else f"{name}{{{','.join(f'{k}={v}' for k, v in labels)}}}": count
            for (name, labels), count in self._counters.items()
        }
