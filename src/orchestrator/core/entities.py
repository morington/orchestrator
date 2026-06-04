from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, conint, conlist, field_validator

from orchestrator.core.enums import (
    DependencyPolicy,
    FilterAction,
    LaunchDefault,
    OnFailure,
    TransportMode,
)

PIPELINE_VERSION_1_0 = "1.0"


class _Strict(BaseModel):
    """Базовая модель публичного DSL: запрещает неизвестные поля (additionalProperties: false)."""

    model_config = ConfigDict(extra="forbid")


class BaseFilter(_Strict):
    """
    Базовый класс фильтра.

    Attributes:
        filter_id: Уникальный идентификатор фильтра.
        condition: Условие для проверки (например, "$1:result == 42").
    """

    filter_id: int = Field(..., ge=1, description="Уникальный идентификатор фильтра")
    condition: str = Field(..., description="Условное выражение для проверки")


class SkipFilterAction(BaseFilter):
    """Фильтр с действием SKIP — пропустить шаг."""

    then: Literal[FilterAction.SKIP] = Field(..., description="Действие при выполнении условия")


class EndFilterAction(BaseFilter):
    """Фильтр с действием END — завершить workflow успешно."""

    then: Literal[FilterAction.END] = Field(..., description="Действие при выполнении условия")


class ErrorFilterAction(BaseFilter):
    """Фильтр с действием ERROR — завершить workflow с ошибкой."""

    then: Literal[FilterAction.ERROR] = Field(..., description="Действие при выполнении условия")


class GotoFilterAction(BaseFilter):
    """Фильтр с действием GOTO — активировать переход к указанным шагам."""

    then: Literal[FilterAction.GOTO] = Field(..., description="Действие при выполнении условия")
    targets: conlist(conint(ge=1), min_length=1) = Field(
        ..., description="Список идентификаторов шагов для перехода",
    )


FilterEntity = Union[SkipFilterAction, EndFilterAction, ErrorFilterAction, GotoFilterAction]


class MiddlewareMeta(_Strict):
    """
    Действия middleware (before/after).

    Attributes:
        set: Пара ключ/значение для добавления или изменения.
        remove: Список ключей для удаления.
    """

    set: dict[str, Any] | None = Field(default=None, description="Ключи для установки/обновления")
    remove: list[str] | None = Field(default=None, description="Ключи для удаления")


class MiddlewareEntity(_Strict):
    """
    Middleware-преобразования данных шага.

    Attributes:
        before: Преобразования до выполнения шага.
        after: Преобразования после выполнения шага.
    """

    before: MiddlewareMeta | None = Field(default=None, description="Middleware до выполнения шага")
    after: MiddlewareMeta | None = Field(default=None, description="Middleware после выполнения шага")


class RetryPolicy(_Strict):
    """
    Политика повторов и таймеров шага (см. docs/SEMANTICS.md §3).

    Attributes:
        max_attempts: Максимум попыток (включая первую).
        dispatch_timeout_sec: Таймаут publish до JetStream ack.
        result_wait_timeout_sec: Таймаут ожидания результата на orchestrator.results.
        request_timeout_sec: Таймаут только для request_reply.
        backoff_sec: Задержки между попытками.
        retryable: Классы ошибок, при которых попытка повторяется.
    """

    max_attempts: int = Field(default=1, ge=1, description="Максимум попыток")
    dispatch_timeout_sec: int = Field(default=10, ge=1, description="Таймаут доставки до ack")
    result_wait_timeout_sec: int = Field(default=30, ge=1, description="Таймаут ожидания результата")
    request_timeout_sec: int = Field(default=15, ge=1, description="Таймаут request_reply")
    backoff_sec: list[int] = Field(default_factory=list, description="Задержки между попытками")
    retryable: list[str] = Field(
        default_factory=lambda: ["dispatch_failed", "result_timeout", "transient_error"],
        description="Классы ретраемых ошибок",
    )


class StepMeta(_Strict):
    """
    Метаданные шага: целевой сервис, транспорт, политики повторов и отказа.

    Attributes:
        target: Subject/имя целевого сервиса для вызова.
        transport_mode: Способ доставки вызова.
        retry_policy: Политика повторов и таймеров.
        on_failure: Политика после исчерпания попыток/TTL.
        valid_for_sec: Абсолютный бизнес-TTL узла от первого ENQUEUED.
        default: Действие по умолчанию (совместимость с фильтрами).
    """

    target: str = Field(..., description="Название целевого сервиса/функции")
    transport_mode: TransportMode = Field(
        default=TransportMode.ASYNC_RESULT_SUBJECT, description="Способ доставки вызова",
    )
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy, description="Политика повторов и таймеров")
    on_failure: OnFailure = Field(default=OnFailure.FAIL, description="Политика после исчерпания попыток")
    valid_for_sec: int | None = Field(default=None, ge=1, description="Абсолютный TTL узла (сек)")
    default: LaunchDefault = Field(default=LaunchDefault.GO, description="Действие по умолчанию")


class DependsOnSpec(_Strict):
    """
    Зависимость шага от узла с политикой готовности.

    Attributes:
        node: step_id upstream-узла.
        policy: Политика готовности (requires_success / requires_closed / optional).
    """

    node: int = Field(..., ge=1, description="step_id upstream-узла")
    policy: DependencyPolicy = Field(
        default=DependencyPolicy.REQUIRES_SUCCESS, description="Политика готовности зависимости",
    )


class OutputSpec(_Strict):
    """
    Описание элемента итоговых outputs workflow (только reference mapping).

    Attributes:
        ref: Ссылка вида "$node:path".
        required: Обязательность; при отсутствии ref → instance FAILED (OUTPUT_MISSING).
        default: Значение по умолчанию для необязательных outputs.
    """

    ref: str = Field(..., description="Ссылка $node:path на результат шага")
    required: bool = Field(default=True, description="Обязателен ли output")
    default: Any = Field(default=None, description="Значение по умолчанию, если ref недоступен")

    @field_validator("ref")
    @classmethod
    def _ref_must_be_reference(cls, value: str) -> str:
        if not value.startswith("$"):
            raise ValueError("output ref must be a reference '$node:path'")
        return value


class StepEntity(_Strict):
    """
    Шаг публичного определения workflow (DSL 1.0).

    Attributes:
        step_id: Уникальный идентификатор шага в рамках определения.
        meta: Метаданные шага.
        depends_on: Зависимости (int или {node, policy}); нормализуются к DependsOnSpec.
        data: Входные данные шага (могут содержать $ref).
        filters: Фильтры маршрута.
        middlewares: Middleware-преобразования.
    """

    step_id: int = Field(..., ge=1, description="Уникальный идентификатор шага")
    meta: StepMeta = Field(..., description="Метаданные шага")
    depends_on: list[DependsOnSpec] | None = Field(default=None, description="Зависимости шага")
    data: dict[str, Any] = Field(default_factory=dict, description="Входные данные шага")
    filters: list[FilterEntity] | None = Field(default=None, description="Фильтры маршрута")
    middlewares: MiddlewareEntity | None = Field(default=None, description="Middleware-преобразования")

    @field_validator("depends_on", mode="before")
    @classmethod
    def _normalize_depends_on(cls, value: Any) -> Any:
        if value is None:
            return None
        normalized: list[Any] = []
        for item in value:
            if isinstance(item, int):
                normalized.append({"node": item, "policy": DependencyPolicy.REQUIRES_SUCCESS})
            else:
                normalized.append(item)
        return normalized


class WorkflowDefinition(_Strict):
    """
    Публичное определение workflow (вариант A: workflow = definition).

    Attributes:
        pipeline_version: Версия публичного DSL.
        definition_key: Тип/шаблон маршрута.
        business_ref: Опциональный клиентский correlation (не используется для dedup).
        idempotency_key: Ключ дедупликации старта → один run_id.
        outputs: Карта итогов workflow (только reference mapping).
        steps: Шаги определения.
    """

    pipeline_version: str = Field(default=PIPELINE_VERSION_1_0, description="Версия публичного DSL")
    definition_key: str = Field(..., min_length=1, description="Тип/шаблон маршрута")
    business_ref: str | None = Field(default=None, description="Клиентский correlation (не dedup)")
    idempotency_key: str = Field(..., min_length=1, description="Ключ дедупликации старта")
    outputs: dict[str, OutputSpec] = Field(default_factory=dict, description="Итоги workflow")
    steps: conlist(StepEntity, min_length=1) = Field(..., description="Шаги определения")

    @field_validator("outputs", mode="before")
    @classmethod
    def _normalize_outputs(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized: dict[str, Any] = {}
        for key, spec in value.items():
            if isinstance(spec, str):
                normalized[key] = {"ref": spec, "required": True}
            else:
                normalized[key] = spec
        return normalized
