# SEMANTICS — норматив семантики исполнения

Этот документ — **источник правды** по семантике оркестратора. Код, тесты и контракты
обязаны соответствовать ему. Главный тезис: оркестратор **не знает домен сервисов**, но
**обязан строго знать семантику собственного исполнения**.

`runtime_version: 1.0`.

---

## §1. Модель A: Workflow = definition, Run = execution

| Термин | Значение | Поле |
|--------|----------|------|
| Workflow (definition) | Тип/шаблон маршрута | `definition_key` (`order.fulfillment`) |
| Run (execution) | Один прогон | `run_id` (генерирует оркестратор) |

Один `definition_key` → много `run_id`. Термин `workflow_id` **не используется** в
runtime/wire (путаница с моделью B). В публичном JSON: `definition_key` + `run_id` в
ответах; опционально `business_ref` (клиентский correlation, **не** dedup).

**Start dedup (детерминированно):**

| Условие | Действие |
|---------|----------|
| Тот же `idempotency_key` | Вернуть существующие `definition_key`, `run_id`, `instance_status` |
| Новый `idempotency_key` | Новый run + новый `run_id` (тот же `definition_key` допустим) |

Инвариант БД: `UNIQUE(idempotency_key)`.

---

## §2. `closed` и `success`

```
SUCCESS = { COMPLETED }
CLOSED  = { COMPLETED, SKIPPED, SKIPPED_BY_GOTO, ABANDONED, EXPIRED, FAILED, CANCELLED, DISPATCH_FAILED }
```

Не-terminal (не в CLOSED): `PENDING`, `RUNNABLE`, `ENQUEUED`, `DISPATCHED`,
`WAITING_RESULT`, `DISPATCH_ERROR`, `CANCELLING`.

`DISPATCH_ERROR` — **временное**, retryable; не closed, не success. Downstream не стартует,
пока узел в `DISPATCH_ERROR` или на retry-пути.

`DISPATCH_FAILED` — **только финальное** terminal (после исчерпания dispatch-retries /
`valid_for_sec`).

**Матрица готовности upstream (только terminal states):**

| Upstream | `requires_success` | `requires_closed` | `optional` |
|----------|:------------------:|:-----------------:|:----------:|
| `COMPLETED` | yes | yes | yes |
| `SKIPPED` | no | yes | yes |
| `SKIPPED_BY_GOTO` | no | yes | yes |
| `ABANDONED` | no | yes | yes |
| `EXPIRED` | no | yes | yes |
| `FAILED` | no | yes | yes |
| `CANCELLED` | no | yes | yes |
| `DISPATCH_FAILED` | no | yes | yes |

`optional` не блокирует запуск ни при каком статусе upstream.

---

## §3. Таймеры

| Поле | Охват |
|------|-------|
| `dispatch_timeout_sec` | publish до JetStream ack |
| `result_wait_timeout_sec` | `WAITING_RESULT` → `orchestrator.results` |
| `request_timeout_sec` | только `request_reply` |
| `backoff_sec` | между attempts |
| `max_attempts` | включая первую |
| `valid_for_sec` | абсолютный бизнес-TTL узла |

**`valid_for_sec` (однозначно):**

- Отсчёт от **первого** перехода в `ENQUEUED` (`enqueued_at`), не от каждой attempt.
- При `now > enqueued_at + valid_for_sec` → `EXPIRED` + `on_failure`, даже если остались
  попытки по `max_attempts` / `backoff_sec`.
- `backoff_sec` и `max_attempts` работают **внутри** окна `valid_for_sec`, не продлевают его.

`deadline_at` для attempt = min(`enqueued_at + valid_for_sec`, per-attempt dispatch/result timers).

---

## §4. FSM: `DISPATCH_ERROR` vs `DISPATCH_FAILED`

**Happy path:**

```
ENQUEUED -> DISPATCHED -> WAITING_RESULT -> COMPLETED
```

**Dispatch retry (временная ошибка):**

```
ENQUEUED -> DISPATCHED -> DISPATCH_ERROR -> ENQUEUED   # attempt+1 после backoff
```

Повтор, пока `attempt < max_attempts` и не истёк `valid_for_sec`.

**Исчерпание dispatch retries:**

```
... -> DISPATCH_ERROR -> DISPATCH_FAILED   # terminal, входит в CLOSED
```

Далее `on_failure` (fail / skip / abandon). **Нет** состояния «и closed, и retryable».

**`fire_and_forget`:** `DISPATCHED` + ack ok → `COMPLETED` (synthetic result
`{ "status": "dispatched" }`); иначе `DISPATCH_ERROR` loop → terminal `DISPATCH_FAILED`.
`WAITING_RESULT` для `fire_and_forget` не используется.

Полная матрица переходов — `src/orchestrator/core/state_machine.py`:

```
PENDING        -> RUNNABLE | SKIPPED | SKIPPED_BY_GOTO | CANCELLED
RUNNABLE       -> ENQUEUED | SKIPPED | SKIPPED_BY_GOTO | CANCELLED
ENQUEUED       -> DISPATCHED | CANCELLED | EXPIRED
DISPATCHED     -> WAITING_RESULT | COMPLETED | DISPATCH_ERROR | EXPIRED
WAITING_RESULT -> COMPLETED | FAILED | ABANDONED | EXPIRED | SKIPPED | CANCELLING | ENQUEUED
DISPATCH_ERROR -> ENQUEUED | DISPATCH_FAILED | EXPIRED
CANCELLING     -> CANCELLED
```

Переход `WAITING_RESULT -> ENQUEUED` — повтор при транзиентном результате
(`failure_class != business`, остались попытки и не истёк `valid_for_sec`): новый
`attempt` + новый `step_run_id`.

`RUNNING` как статус **не используется** (заменён на `WAITING_RESULT`).
`RUNNABLE` — scheduler выбрал узел; `ENQUEUED` — outbox-запись создана.

---

## §4b. Запрещено в 1.0

| Feature | Статус | Причина |
|---------|--------|---------|
| Loop (`while` / `for-each` / `until`) | unsupported | validator/compiler reject |
| Dynamic nodes (runtime graph mutation) | forbidden | ломает runtime |
| Child workflows / sub-pipeline | `NotImplemented` | parent/child семантика не готова |

`NodeType` enum содержит `loop` / `sub_pipeline` для схемы БД, но validator + compiler
reject их в `pipeline_version: 1.0`.

---

## §5. Late result (после terminal узла)

Если inbox-result приходит, когда узел уже в
`{ EXPIRED, ABANDONED, FAILED, CANCELLED, COMPLETED, SKIPPED }`:

→ `late_result`: DLQ + метрика `inbox_late_result_total`; узел **не** переоткрывается,
FSM не меняется. Сообщение **ack** (не nack-loop).

Исключение: `COMPLETED` + дубликат `(step_run_id, attempt)` — это `duplicate` (§6), не late.

---

## §6. Transport, invoke, outputs, retention

- **Transport modes:** `async_result_subject` (дефолт prod), `request_reply`,
  `fire_and_forget` (synthetic result).
- **Идентичность вызова:** `step_run_id` (стабилен на весь логический шаг), `attempt`
  (1..N), `delivery_id` (на одну outbox-запись → `Nats-Msg-Id`), `node_key` (из графа).
- **Inbox dedup:** `UNIQUE(step_run_id, attempt)`.

| Ситуация | Действие |
|----------|----------|
| Тот же `(step_run_id, attempt)`, повторное тело | `duplicate` → ack, state без изменений |
| `attempt` < текущего у узла | `stale` → DLQ + метрика |
| `attempt` == текущего, узел `WAITING_RESULT` | apply transition |

- **Outputs:** только reference mapping `$node:path` (+ `required` / `default`). Никаких
  `concat`/`if`/`template`/выражений — любая трансформация это отдельный task-шаг.
  `required: true` и ref недоступен → instance `FAILED` (`OUTPUT_MISSING`);
  `required: false` → подставить `default`.
- **Retention инварианты (§7 production):** никогда не удалять non-terminal instances;
  никогда не удалять rows с активным lease (`locked_until > now()`); не удалять
  outbox/inbox/DLQ, пока parent instance non-terminal.

---

## §7. Терминология: pipeline (public) vs workflow (runtime)

| Слой | Термины | Запрещено |
|------|---------|-----------|
| Public DSL / JSON Schema / README | `pipeline_version`, `steps[]`, subject `workflow.*` | `pipeline_id` |
| Ingress DTO | `WorkflowDefinition` (Pydantic, `entities.py`) | `PipelineEntity` |
| Runtime / domain / store | `WorkflowInstance`, `ExecutionGraph`, `ExecutionNode`, `WorkflowRuntime`, `WorkflowStore` | `Pipeline*` |

Ingress pipeline (обязательный):

```
WorkflowDefinition -> WorkflowDefinitionValidator -> GraphCompiler -> ExecutionGraph
```

Validator проверяет: циклы, висящие `depends_on`, unknown `$ref`, invalid GOTO targets,
дубли `step_id`, поддерживаемую версию, запрещённые конструкции.

`pipeline_version` — версия публичного DSL; `compiled_graph_version` / `runtime_version` —
metadata run; `message_version` — wire-envelope.

---

## §8. GOTO = compile-time edge activation

GOTO в фильтре **не** мутирует runtime-список шагов. При срабатывании GOTO runtime
активирует target-edges; узлы не в targets и зависящие только от обойдённой ветки →
`SKIPPED_BY_GOTO` (явный статус, не `CANCELLED`, не `NOT_VISITED`).

Пример `A→B→C→D`, `A GOTO D`: `B`, `C` → `SKIPPED_BY_GOTO`; `D` runnable.

---

## §9. Graph snapshot (anti-drift)

Хранятся одновременно `definition_snapshot_json` (аудит), `execution_nodes`,
`execution_edges`. Единственный источник для scheduler/runtime — `execution_nodes` +
`execution_edges`. `definition_snapshot_json` — только аудит; граф для существующего
`run_id` **никогда** не пересобирается. Попытка recompile для active run → domain error.

На instance: `definition_hash` (SHA256 canonical JSON) + `definition_revision`
(опционально) + `pipeline_version`.

---

## §10. `workflow.completed`

- Публикуется **ровно один раз** на `run_id`.
- Инвариант: `workflow_instances.completion_published_at IS NOT NULL` **или** outbox-row
  `kind=workflow_completed` со статусом `sent`.
- Повторное завершение (schedule / recovery) → no-op publish, идемпотентно по `run_id`.

---

## §11. Payload

Если `len(payload) > PAYLOAD_INLINE_MAX` (config, дефолт 256KB) → `blob_ref` (URI), inline
в JSONB не хранить. Применяется к `data`, `result`. NATS default max ~1 MiB/сообщение.

---

## §12. Admin `retry_node`

Только **terminal leaf node** — узел без downstream или со всеми downstream
terminal/skipped. Retry промежуточного узла (инвалидировал бы downstream) запрещён в 1.0.

---

## Gate

Код разрешён к написанию после фиксации §4 (`DISPATCH_ERROR` / `DISPATCH_FAILED`).
Этот документ зафиксирован; реализация ему соответствует.
