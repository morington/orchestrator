# OPERATIONS — эксплуатация

Admin-операции, отмена, recovery, retention и метрики оркестратора.

## Admin-subjects (`orchestrator.admin.*`)

Тело команды — JSON с `run_id` (и `node_key` где применимо). Ответ содержит
`runtime_version`, `definition_key`, `run_id` и сводку узлов; внутренние ID узлов —
только в логах.

| Subject | Параметры | Действие |
| --- | --- | --- |
| `orchestrator.admin.inspect` | `run_id` | сводка run и статусов узлов |
| `orchestrator.admin.retry_node` | `run_id`, `node_key` | сброс **terminal leaf**-узла и перепланирование |
| `orchestrator.admin.resume` | `run_id` | повторно продвинуть run (после ручного вмешательства) |
| `orchestrator.admin.abandon_node` | `run_id`, `node_key` | пометить узел `ABANDONED` и продолжить |
| `orchestrator.admin.cancel` | `run_id`, `reason?` | отмена run |

`retry_node` в 1.0 поддерживает только terminal leaf-узел (все нисходящие закрыты);
сброс возвращает узел в `PENDING` (новый attempt) и снимает терминальный статус run.

## Отмена (`orchestrator.workflow.cancel`)

Тело: `run_id`, опционально `reason`. Незапущенные узлы (`PENDING`/`RUNNABLE`/`ENQUEUED`)
→ `CANCELLED`; ожидающие результат (`WAITING_RESULT`) → cooperative cancel через
`CANCELLING → CANCELLED`. По завершении публикуется `workflow.completed` со статусом
`cancelled`. Отмена уже терминального run — no-op.

## Recovery при старте

`RecoveryOnStartup` выполняет при запуске процесса:

1. `reclaim_expired_leases` — снять истёкшие leases с узлов (lease забрала упавшая реплика);
2. `OutboxPublisher.run_once` — дослать pending outbox (дубли исключены `Nats-Msg-Id`);
3. `TimeoutWatcher.run_once` — обработать просроченные узлы (`deadline_at < now`).

NATS дренится при остановке (`gateway.drain`), что уменьшает объём orphaned-состояния.

## Фоновые воркеры

| Воркер | Период (`.env`) | Назначение |
| --- | --- | --- |
| `OutboxPublisher` | `ENGINE__OUTBOX_POLL_SEC` | публикация outbox (claim + `SKIP LOCKED`) |
| `TimeoutWatcher` | `ENGINE__TIMEOUT_POLL_SEC` | EXPIRED по `deadline_at` + `on_failure` |
| `RetentionCleanupWorker` | `RETENTION__POLL_SEC` | очистка terminal-данных |

## Retention

Политика очистки (`RETENTION__*`): `workflow_days`, `outbox_days`, `inbox_days`,
`dlq_days`. Инварианты: не удаляются non-terminal instances и строки с активным lease.

## Конкурентность и leases

- Узел захватывается через `locked_by` / `locked_until` + `revision` (оптимистичная блокировка).
- `claim_outbox` использует `FOR UPDATE SKIP LOCKED` (PostgreSQL) — несколько реплик
  не доставляют одно сообщение дважды.
- `lease_ttl_sec` (`ENGINE__LEASE_TTL_SEC`) — TTL захвата; по истечении lease переотбирается.

## Метрики

Низкокардинальные счётчики (`Metrics`), допустимые метки: `status`, `node_type`,
`transport_mode`, `target_group`, `failure_class`, `message_version`, `kind`, `table`.
Запрещены высококардинальные метки (`run_id`, `node_key`, `step_run_id`). Текущая
реализация — in-memory счётчики (`Metrics.snapshot()`); экспорт в Prometheus —
точка расширения.

## Миграции БД

Схема управляется Alembic. В Docker миграции применяет отдельный контейнер
`migration` (`dockerfiles/Dockerfile.migration`, `ENTRYPOINT alembic`): он зависит от
healthy-PostgreSQL, выполняет `upgrade head` и завершается; оркестратор стартует только
после его успешного завершения.

```bash
make migrate        # через контейнер (docker compose up --build migration)
make migrate-local  # локально: uv run alembic upgrade head (STORAGE__URL на localhost)
```

Новая ревизия: `uv run alembic revision --autogenerate -m "<описание>"`, затем правка и
`upgrade head`. URL подключения берётся из `STORAGE__URL` (см. `alembic/env.py`).

## DLQ

Poison-сообщения (битый JSON, неизвестная версия, неизвестный `run_id`/узел) уходят в
dead-letter (`orchestrator.deadletter` / таблица `dead_letter_messages`) вместо
бесконечного nack. Для разбора используется `inspect` и повторная публикация из DLQ.
