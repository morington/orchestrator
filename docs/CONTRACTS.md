# CONTRACTS — норматив для интеграторов

Документ фиксирует wire-контракт между оркестратором и микросервисами. Нормативные
значения версий — в `orchestrator.app.domain.contracts`.

## Версионирование

- `message_version` — строка SemVer `major.minor` (текущая `"1.0"`).
  - `1.1` — обратно совместимые поля; `2.0` — breaking.
- `CURRENT_MESSAGE_VERSION = "1.0"`, `SUPPORTED_MESSAGE_VERSIONS = frozenset({"1.0"})`.
- Сообщение с неподдерживаемой версией на `start` **отклоняется** (run не создаётся);
  на `orchestrator.results` — уходит в DLQ (не nack-loop).

## Subjects

| Subject | Кто публикует | Кто слушает |
| --- | --- | --- |
| `orchestrator.workflow.start` | клиент | оркестратор |
| `<meta.target>` | оркестратор | микросервис |
| `orchestrator.results` | микросервис | оркестратор |
| `orchestrator.workflow.completed` | оркестратор | подписчики событий |
| `orchestrator.workflow.cancel` | клиент | оркестратор |
| `orchestrator.admin.*` | оператор | оркестратор |
| `orchestrator.deadletter` | оркестратор | DLQ-обработчик |

## Вызов шага — `InvokeEnvelope` (egress на `meta.target`)

```json
{
  "message_version": "1.0",
  "definition_key": "order.fulfillment",
  "run_id": "<ulid>",
  "node_key": "step-2",
  "step_run_id": "<ulid>",
  "attempt": 1,
  "transport_mode": "async_result_subject",
  "reply_subject": "orchestrator.results",
  "data": { "text": "..." }
}
```

JSON Schema: [`schemas/message-1.0.schema.json`](../schemas/message-1.0.schema.json).

## Результат шага — `StepResultMessage` (ingress на `orchestrator.results`)

```json
{
  "message_version": "1.0",
  "definition_key": "order.fulfillment",
  "run_id": "<ulid>",
  "node_key": "step-2",
  "step_run_id": "<ulid>",
  "attempt": 1,
  "result": { "text": "..." }
}
```

Ошибка вместо `result`:

```json
{ "error": { "failure_class": "business", "message": "not found" } }
```

JSON Schema: [`schemas/step-result-1.0.schema.json`](../schemas/step-result-1.0.schema.json).

### Требования к сервису

- **Эхо**: вернуть `run_id`, `node_key`, `step_run_id`, `attempt` без изменений.
- **Идемпотентность**: по `step_run_id`. Оркестратор дополнительно дедуплицирует через
  inbox по stable operation id; повтор того же `step_run_id`/`attempt` безопасен.
- **Stale attempt**: результат с устаревшим `attempt` игнорируется.
- `failure_class`: `business` (логический отказ — применяется `on_failure`) или
  `transient` (временный — возможен ретрай по `retry_policy`).

## Большие payload — `blob_ref`

Если `len(payload) > STORAGE__PAYLOAD_INLINE_MAX` (например 256 KB), вместо inline в
`data` / `result` передаётся `blob_ref` (URI на внешнее хранилище). Inline-хранение
больших полей в JSONB запрещено. Сервисы могут как принимать, так и отдавать `blob_ref`.

## Модель исполнения (SM vs workflow)

Сейчас исполняется DAG task-узлов (state machine по графу). Движок workflow-ready:
`ExecutionGraph`/`ExecutionNode` готовы к fork/join/циклам/суб-пайплайнам (roadmap),
но в 1.0 эти конструкции запрещены валидатором.

## `on_failure` / `valid_for_sec`

| Сценарий | `on_failure` | `valid_for_sec` | Поведение |
| --- | --- | --- | --- |
| Критичный шаг | `fail` | `300` | отказ/таймаут → run FAILED |
| Эфемерный notify | `abandon` | `60` | отказ/таймаут → узел брошен, run продолжается |
| Необязательный шаг | `skip` | — | отказ → узел пропущен, нисходящие по `requires_closed` идут дальше |

## Completion event (`orchestrator.workflow.completed`)

- Публикуется **ровно один раз** на `run_id` (идемпотентность через
  `completion_published_at`).
- При recovery повторная публикация исключена: outbox-запись дедуплицируется по
  `delivery_id` (`Nats-Msg-Id`).
- Тело: `definition_key`, `run_id`, `status`, `outputs`, `failure_reason`.

## Migration note (со старых интеграций docs-main / MVP)

| Старое | Новое | Примечание |
| --- | --- | --- |
| `pipeline_id` / `workflow_id` | `definition_key` | `pipeline_id` **не принимается** в schema/DTO/subjects |
| subject `orchestrator.result` | `orchestrator.results` | переименование |
| subject `orchestrator.pipeline.start` | `orchestrator.workflow.start` | переименование |
| поле `output` | `result` | канон — `result` |

Публичный язык не содержит `pipeline_id` нигде, кроме этой таблицы миграции.
