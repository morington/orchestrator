from enum import StrEnum, auto


class LaunchDefault(StrEnum):
    """
    Действие по умолчанию для шага, если фильтры не приняли решения.

    Attributes:
        GO: запускать шаг.
        SKIP: пропускать шаг.
    """

    GO = auto()
    SKIP = auto()


class FilterAction(StrEnum):
    """
    Возможные действия фильтра маршрута.

    Attributes:
        SKIP: пропустить шаг.
        GOTO: активировать переход к указанным узлам.
        END: завершить workflow успешно.
        ERROR: завершить workflow с ошибкой.
    """

    SKIP = auto()
    GOTO = auto()
    END = auto()
    ERROR = auto()


class TransportMode(StrEnum):
    """
    Способ доставки вызова шага в микросервис.

    Attributes:
        ASYNC_RESULT_SUBJECT: publish, ответ приходит на orchestrator.results.
        REQUEST_REPLY: NATS request с inline-ответом.
        FIRE_AND_FORGET: publish-only, без ожидания результата.
    """

    ASYNC_RESULT_SUBJECT = "async_result_subject"
    REQUEST_REPLY = "request_reply"
    FIRE_AND_FORGET = "fire_and_forget"


class DependencyPolicy(StrEnum):
    """
    Политика готовности зависимости по статусу upstream-узла.

    Attributes:
        REQUIRES_SUCCESS: upstream обязан быть COMPLETED.
        REQUIRES_CLOSED: достаточно любого terminal-статуса upstream.
        OPTIONAL: зависимость не блокирует запуск.
    """

    REQUIRES_SUCCESS = "requires_success"
    REQUIRES_CLOSED = "requires_closed"
    OPTIONAL = "optional"


class OnFailure(StrEnum):
    """
    Политика обработки узла после исчерпания попыток или TTL.

    Attributes:
        FAIL: узел FAILED, весь workflow FAILED.
        SKIP: узел SKIPPED, зависимые обрабатываются по depends_on.
        ABANDON: узел ABANDONED, workflow продолжается.
    """

    FAIL = "fail"
    SKIP = "skip"
    ABANDON = "abandon"


class FailureClass(StrEnum):
    """
    Класс ошибки в результате шага — влияет на retry.

    Attributes:
        BUSINESS: бизнес-ошибка, не ретраится.
        TRANSIENT: временная ошибка, ретраится.
        DISPATCH_FAILED: ошибка доставки вызова.
    """

    BUSINESS = "business"
    TRANSIENT = "transient"
    DISPATCH_FAILED = "dispatch_failed"


class NodeType(StrEnum):
    """
    Тип узла графа. В pipeline_version 1.0 разрешён только TASK.

    Attributes:
        TASK: обычный шаг с вызовом сервиса.
        FORK: ветвление (roadmap).
        JOIN: слияние веток (roadmap).
        LOOP: цикл (forbidden 1.0).
        SUB_PIPELINE: подпайплайн (forbidden 1.0).
    """

    TASK = "task"
    FORK = "fork"
    JOIN = "join"
    LOOP = "loop"
    SUB_PIPELINE = "sub_pipeline"


class EdgeKind(StrEnum):
    """
    Тип ребра графа исполнения.

    Attributes:
        DEPENDENCY: обычная зависимость depends_on.
        GOTO: ребро активации перехода (по умолчанию неактивно).
        FORK: ребро ветвления (roadmap).
        JOIN: ребро слияния (roadmap).
        LOOP: обратное ребро (roadmap).
    """

    DEPENDENCY = "dependency"
    GOTO = "goto"
    FORK = "fork"
    JOIN = "join"
    LOOP = "loop"
