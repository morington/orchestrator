class DependencyError(Exception):
    """Базовое исключение для ошибок зависимостей."""


class DependencyFormatError(DependencyError):
    """Неверный формат ссылки на зависимость."""


class IndexOutOfRangeError(DependencyError):
    """Индекс выходит за пределы списка."""


class KeyNotFoundError(DependencyError):
    """Ключ не найден в словаре."""


class TypeMismatchError(DependencyError):
    """Несоответствие ожидаемого и фактического типа данных."""


class FilterError(Exception):
    """Базовое исключение для ошибок фильтрации."""


class FilterSyntaxError(FilterError):
    """Синтаксическая ошибка в выражении фильтра."""


class FilterUnsupportedSyntaxError(FilterError):
    """Использована неподдерживаемая синтаксическая конструкция."""


class FilterUnsupportedOperatorError(FilterError):
    """Использован неподдерживаемый оператор."""


class FilterUnknownIdentifierError(FilterError):
    """Обнаружен неизвестный идентификатор."""


class FilterMissingStepError(FilterError):
    """Отсутствует шаг, на который ссылается фильтр."""


class FilterDependencyError(FilterError):
    """Ошибка разрешения dependency внутри фильтра."""


class FilterEvaluationError(FilterError):
    """Неожиданная ошибка вычисления условия."""


class MiddlewareError(Exception):
    """Базовое исключение для ошибок middleware."""


class MiddlewareDependencyError(MiddlewareError):
    """Ошибка разрешения dependency внутри middleware."""


class MiddlewareTypeError(MiddlewareError):
    """Ошибка типов данных при обработке middleware."""


class DomainError(Exception):
    """Базовое доменное исключение оркестратора."""


class InvalidTransitionError(DomainError):
    """Недопустимый переход в state machine узла или run."""


class WorkflowDefinitionError(DomainError):
    """Невалидное определение workflow (отклонено валидатором)."""


class GraphImmutabilityError(DomainError):
    """Попытка пересобрать граф для уже существующего run_id."""


class UnsupportedVersionError(DomainError):
    """Неизвестная версия контракта (message_version / pipeline_version)."""
