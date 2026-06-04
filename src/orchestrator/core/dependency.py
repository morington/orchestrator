import re
from collections.abc import Mapping
from typing import Any, Union

from orchestrator.core.errors import (
    DependencyFormatError,
    IndexOutOfRangeError,
    KeyNotFoundError,
    TypeMismatchError,
)

_DEP_RE = re.compile(r"^\$(?P<step>\d+):(?P<path>.+)$")
_DEP_LOCAL_RE = re.compile(r"^\$(?P<path>[^:]+)$")


class Dependency:
    """
    Ссылка на результат шага пайплайна или локальные данные.

    Attributes:
        step_id: Идентификатор шага (int) или None для локальной зависимости.
        path: Путь до значения (последовательность сегментов: str или int).
    """

    __slots__ = ("path", "step_id")

    def __init__(self, step_id: int | None, path: tuple[Union[str, int], ...]):
        """
        Инициализация зависимости.

        Args:
            step_id: Идентификатор шага или None для локальной зависимости.
            path: Последовательность сегментов пути.

        Raises:
            DependencyFormatError: Если step_id отрицательный или путь пустой.
        """
        if step_id is not None and step_id < 1:
            raise DependencyFormatError(f"Step id must be positive, got {step_id}")
        if not path:
            raise DependencyFormatError("Path must not be empty")

        self.step_id = step_id
        self.path = path

    @staticmethod
    def resolve_dict(
        data: dict[str, Any],
        local_results: dict[str, Any],
        global_results: dict[int, Any],
    ) -> dict[str, Any]:
        """
        Разрешить зависимости в словаре данных.

        Args:
            data: Словарь, в котором значения могут содержать зависимости (строки "$...").
            local_results: Локальные данные.
            global_results: Глобальные данные по шагам.

        Returns:
            dict[str, Any]: Словарь с разрешёнными значениями.

        Raises:
            DependencyFormatError: Некорректный формат зависимости.
            IndexOutOfRangeError: Индекс вне диапазона.
            KeyNotFoundError: Ключ не найден.
            TypeMismatchError: Неверный тип данных.
        """

        def _resolve_value(value: Any) -> Any:
            if isinstance(value, str):
                dep = Dependency.parse(value)
                if dep:
                    if dep.step_id is not None:
                        return dep.resolve(global_results.get(dep.step_id))
                    return dep.resolve(local_results)
                return value
            if isinstance(value, dict):
                return {k: _resolve_value(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_resolve_value(v) for v in value]
            return value

        return {k: _resolve_value(v) for k, v in data.items()}

    @classmethod
    def parse(cls, s: str) -> "Dependency | None":
        """
        Разобрать строковое представление зависимости.

        Args:
            s: Строка с зависимостью (например: "$1:result.value" или "$foo.bar").

        Returns:
            Dependency | None: Объект зависимости или None, если строка не начинается с "$".

        Raises:
            DependencyFormatError: Если формат строки некорректен.
        """
        if not isinstance(s, str) or not s.startswith("$"):
            return None

        m = _DEP_RE.match(s)
        if m:
            step_id = cls._parse_step_id(m.group("step"), s)
            path = cls._parse_path(m.group("path"), s)
            return cls(step_id=step_id, path=path)

        m = _DEP_LOCAL_RE.match(s)
        if m:
            path = cls._parse_path(m.group("path"), s)
            return cls(step_id=None, path=path)

        raise DependencyFormatError(f"Invalid dependency format: {s}")

    @staticmethod
    def _parse_step_id(raw: str, full: str) -> int:
        """
        Преобразовать строковый идентификатор шага в число.

        Args:
            raw: Строковый идентификатор.
            full: Полная строка зависимости (для сообщений об ошибках).

        Returns:
            int: Числовой идентификатор шага.

        Raises:
            DependencyFormatError: Если идентификатор невалиден.
        """
        try:
            return int(raw)
        except ValueError as e:
            raise DependencyFormatError(f"Invalid step id in {full}") from e

    @staticmethod
    def _parse_path(path_str: str, full: str) -> tuple[str | int, ...]:
        """
        Разобрать строковый путь в последовательность сегментов.

        Args:
            path_str: Строка с путём (например, "foo.bar.0").
            full: Полная строка зависимости (для сообщений об ошибках).

        Returns:
            tuple[str | int, ...]: Последовательность сегментов пути.

        Raises:
            DependencyFormatError: Если сегмент пустой.
        """
        if not path_str:
            raise DependencyFormatError(f"Empty path in dependency: {full}")

        path: list[str | int] = []
        for seg in path_str.split("."):
            if not seg:
                raise DependencyFormatError(f"Empty segment in path: {full}")
            path.append(int(seg) if seg.isdigit() else seg)
        return tuple(path)

    def resolve(self, data: Any) -> Any:
        """
        Разрешить зависимость в конкретном объекте данных.

        Args:
            data: Данные (результат шага или локальный контекст).

        Returns:
            Любое значение из данных по указанному пути.

        Raises:
            IndexOutOfRangeError: Индекс вне диапазона для списка.
            KeyNotFoundError: Ключ отсутствует в словаре.
            TypeMismatchError: Неподходящий тип для сегмента пути.
        """
        cur = data
        for seg in self.path:
            if isinstance(seg, int):
                cur = self._resolve_numeric_segment(cur, seg)
            else:
                cur = self._resolve_key_segment(cur, seg)
        return cur

    def _resolve_numeric_segment(self, cur: Any, seg: int) -> Any:
        """
        Разрешение числового сегмента пути.

        Args:
            cur: Текущие данные.
            seg: Индекс или числовой ключ.

        Returns:
            Значение по указанному индексу или ключу.

        Raises:
            IndexOutOfRangeError: Индекс вне диапазона списка.
            KeyNotFoundError: Ключ отсутствует в словаре.
            TypeMismatchError: Неподходящий тип объекта.
        """
        if isinstance(cur, list):
            try:
                return cur[seg]
            except IndexError as e:
                raise IndexOutOfRangeError(f"Index {seg} out of range in list") from e
        if isinstance(cur, Mapping):
            key = str(seg)
            if key not in cur:
                raise KeyNotFoundError(f"Key '{seg}' not found{self._format_where()}")
            return cur[key]
        raise TypeMismatchError(
            f"Expected list or dict for numeric segment, got {type(cur).__name__}",
        )

    def _resolve_key_segment(self, cur: Any, seg: str) -> Any:
        """
        Разрешение строкового сегмента пути.

        Args:
            cur: Текущие данные.
            seg: Ключ словаря.

        Returns:
            Значение по ключу.

        Raises:
            KeyNotFoundError: Ключ отсутствует в словаре.
            TypeMismatchError: Неподходящий тип объекта.
        """
        if not isinstance(cur, Mapping):
            raise TypeMismatchError(
                f"Expected dict for key '{seg}', got {type(cur).__name__}",
            )
        if seg not in cur:
            raise KeyNotFoundError(f"Key '{seg}' not found{self._format_where()}")
        return cur[seg]

    def _format_where(self) -> str:
        """
        Сформировать контекст для сообщений об ошибках.

        Returns:
            str: Подстрока вида " at step <id>" или пустая строка.
        """
        return f" at step {self.step_id}" if self.step_id is not None else ""

    def __repr__(self) -> str:
        """Отладочное представление объекта зависимости."""
        return f"Dependency(step_id={self.step_id}, path={self.path})"

    @property
    def dependency(self) -> str:
        """
        Строковое представление зависимости (как в конфигурации пайплайна).

        Returns:
            str: Строка зависимости в формате "$<step>:<path>" или "$<path>".
        """
        path_str = ".".join(str(seg) for seg in self.path)
        return f"${self.step_id}:{path_str}" if self.step_id is not None else f"${path_str}"

    def __str__(self) -> str:
        """Строковое представление зависимости."""
        return self.dependency
