import copy
from collections.abc import Mapping
from typing import Any

from orchestrator.core.dependency import Dependency
from orchestrator.core.entities import MiddlewareMeta
from orchestrator.core.errors import (
    DependencyError,
    MiddlewareDependencyError,
    MiddlewareTypeError,
)


class MiddlewareExecutor:
    """
    Выполняет правила middleware: set/remove над словарём данных.

    Поддержка:
        - Установка значений по dot-path (включая автосоздание вложенных dict/list).
        - Удаление значений по dot-path.
        - Значения могут быть литералами или dependency-ссылками:
            * глобальные: "$<step_id>:<path>"  → берутся из steps[step_id]
            * локальные:  "$<path>"            → берутся из local
    """

    def __init__(self, spec: MiddlewareMeta | None, *, auto_list: bool = True) -> None:
        """
        Args:
            spec: Объект описания middleware (set/remove).
            auto_list: Если True — разрешено автосоздание списков.
        """
        self.spec = spec
        self.auto_list = auto_list

    def run(
        self,
        *,
        global_data: Mapping[int, Mapping[str, Any]] | None = None,
        local: Mapping[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Применяет set/remove к словарю и возвращает новый словарь.

        Args:
            global_data: Результаты шагов пайплайна (по step_id).
            local: Локальный контекст.
            data: Исходные данные, к которым применяются изменения.

        Returns:
            dict[str, Any]: Новый словарь с применёнными правилами.

        Raises:
            MiddlewareDependencyError: Ошибка в dependency.
            MiddlewareTypeError: Неправильный тип при установке значений.
        """
        global_data = global_data or {}
        local = local or {}
        target = copy.deepcopy(data or {})

        if self.spec:
            if self.spec.set:
                for key, raw_value in self.spec.set.items():
                    value = self._resolve_value(raw_value, steps=global_data, local=local, target=target)
                    self._set_at_path(target=target, path=key.split("."), value=value)

            if self.spec.remove:
                for path in self.spec.remove:
                    self._remove_at_path(data=target, path=path.split("."))

        return target

    def _resolve_value(
        self,
        raw_value: Any,
        *,
        steps: Mapping[int, Any],
        local: Mapping[str, Any],
        target: dict[str, Any],
    ) -> Any:
        """
        Разрешить значение (литерал или dependency).

        Args:
            raw_value: Исходное значение из spec.set.
            steps: Данные шагов.
            local: Локальный контекст.
            target: Текущий словарь (для локальных ссылок).

        Returns:
            Любое значение (литерал или результат dependency).

        Raises:
            MiddlewareDependencyError: Ошибка в dependency.
        """
        if isinstance(raw_value, str):
            try:
                dep = Dependency.parse(raw_value)
            except DependencyError as e:
                raise MiddlewareDependencyError(f"Invalid dependency: {e}") from e

            if dep is not None:
                try:
                    if dep.step_id is None:
                        data_src = local if isinstance(local, Mapping) and local else target
                        return dep.resolve(data_src)
                    if dep.step_id not in steps:
                        raise MiddlewareDependencyError(f"Missing step {dep.step_id} in results")
                    return dep.resolve(steps[dep.step_id])
                except DependencyError as e:
                    raise MiddlewareDependencyError(f"Dependency resolve error: {e}") from e

        return raw_value

    def _set_at_path(self, target: dict, path: list[str], value: Any) -> None:
        """
        Устанавливает значение по dot-path.

        Автоматически создаёт вложенные dict и list при необходимости.

        Args:
            target: Целевой словарь.
            path: Список сегментов пути.
            value: Устанавливаемое значение.

        Raises:
            MiddlewareTypeError: При несовпадении типов.
        """
        cur: Any = target
        for i, seg in enumerate(path):
            is_last = i == len(path) - 1

            if seg.isdigit():
                cur = self._handle_list_segment(cur=cur, seg=seg, value=value, is_last=is_last)
            else:
                cur = self._handle_dict_segment(cur=cur, seg=seg, value=value, path=path, i=i, is_last=is_last)

    def _handle_list_segment(self, cur: Any, seg: str, value: Any, *, is_last: bool) -> Any:
        """
        Обработка сегмента-числа (list).

        Args:
            cur: Текущий контейнер.
            seg: Сегмент пути (число в строке).
            value: Значение.
            is_last: Признак последнего сегмента.

        Returns:
            Новый текущий контейнер.

        Raises:
            MiddlewareTypeError: Если текущий контейнер не list.
        """
        idx = int(seg)

        if not isinstance(cur, list):
            raise MiddlewareTypeError(
                f"Expected list for numeric segment '{seg}', got {type(cur).__name__}",
            )

        while len(cur) <= idx:
            cur.append(None)

        if is_last:
            cur[idx] = value
            return cur

        if cur[idx] is None:
            cur[idx] = {}
        return cur[idx]

    def _handle_dict_segment(
        self, cur: Any, seg: str, value: Any, path: list[str], i: int, *, is_last: bool,
    ) -> Any:
        """
        Обработка строкового сегмента (dict).

        Args:
            cur: Текущий контейнер.
            seg: Сегмент пути (строка).
            value: Значение.
            path: Полный путь.
            i: Индекс текущего сегмента.
            is_last: Признак последнего сегмента.

        Returns:
            Новый текущий контейнер.

        Raises:
            MiddlewareTypeError: Если текущий контейнер не dict.
        """
        if not isinstance(cur, dict):
            raise MiddlewareTypeError(
                f"Expected dict at segment '{seg}', got {type(cur).__name__}",
            )

        if is_last:
            cur[seg] = value
            return cur

        if seg not in cur:
            next_is_digit = i + 1 < len(path) and path[i + 1].isdigit() and self.auto_list
            cur[seg] = [] if next_is_digit else {}
        return cur[seg]

    @staticmethod
    def _remove_at_path(*, data: dict[str, Any], path: list[str]) -> None:
        """
        Удалить ключ по dot-path (если нет — игнорируем).

        Args:
            data: Целевой словарь.
            path: Список сегментов пути.
        """
        cur = data
        for seg in path[:-1]:
            if isinstance(cur, dict):
                if seg not in cur:
                    return
                cur = cur[seg]
            elif isinstance(cur, list) and seg.isdigit():
                idx = int(seg)
                if idx >= len(cur):
                    return
                cur = cur[idx]
            else:
                return

        last = path[-1]
        if isinstance(cur, dict):
            cur.pop(last, None)
        elif isinstance(cur, list) and last.isdigit():
            idx = int(last)
            if idx < len(cur):
                cur[idx] = None
