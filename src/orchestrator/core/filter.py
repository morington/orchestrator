import ast
import operator
import re
from collections.abc import Mapping
from typing import Any

from orchestrator.core.dependency import Dependency
from orchestrator.core.errors import (
    DependencyError,
    FilterDependencyError,
    FilterError,
    FilterEvaluationError,
    FilterMissingStepError,
    FilterSyntaxError,
    FilterUnknownIdentifierError,
    FilterUnsupportedOperatorError,
    FilterUnsupportedSyntaxError,
)

_ALLOWED_OPERATORS: dict[type, Any] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
}
# Оборачиваем $refs в кавычки, чтобы парсер видел их как строки
_DEP_PATTERN = re.compile(r"(\$[0-9]+:[A-Za-z0-9_.-]+|\$[A-Za-z_][A-Za-z0-9_.-]*)")


class ConditionEvaluator:
    """
    Безопасный вычислитель булевых условий с поддержкой Dependency-ссылок.

    Поддержка:
        - Сравнения: ==, !=, <, >, <=, >=, is, is not (цепочки допустимы, как в Python)
        - Логика: and, or, not
        - Скобки
        - Литералы: числа, строки, True/False/None и их нижний регистр (true/false/null)
        - Dependency refs:
            * глобальные: "$<step_id>:<path>"  → берутся из steps[step_id]
            * локальные:  "$<path>"            → берутся из local

    Безопасность:
        - Жёсткая валидация AST (никаких вызовов функций, индексов, атрибутов)
        - Ошибки приводятся к семейству Filter* ошибок
    """

    def evaluate(
        self,
        condition: str,
        global_data: Mapping[int, Mapping[str, Any]] | None = None,
        local: Mapping[str, Any] | None = None,
    ) -> bool:
        """
        Вычислить условие.

        Args:
            condition: Строковое выражение условия.
            global_data: Результаты шагов пайплайна (по step_id).
            local: Локальный контекст (для $refs без step_id).

        Returns:
            bool: Результат вычисления.

        Raises:
            FilterSyntaxError: Синтаксическая ошибка.
            FilterUnsupportedSyntaxError: Обнаружена неподдерживаемая конструкция.
            FilterUnsupportedOperatorError: Использован запрещённый оператор.
            FilterUnknownIdentifierError: Неизвестный идентификатор.
            FilterMissingStepError: Отсутствует шаг из глобальной dependency.
            FilterDependencyError: Ошибка при парсинге/резолве dependency.
            FilterEvaluationError: Прочие ошибки вычисления.
        """
        try:
            processed = self._preprocess(condition)
            tree = self._parse(processed)
            self._validate_ast(tree)
            result = self._eval(tree.body, steps=global_data or {}, local=local or {})
            if not isinstance(result, bool):
                raise FilterUnsupportedSyntaxError(
                    f"Expression must evaluate to boolean, got {type(result).__name__}",
                )
            return result
        except FilterError:
            raise
        except DependencyError as e:
            raise FilterDependencyError(f"Dependency error: {e}") from e
        except SyntaxError as e:
            raise FilterSyntaxError(f"Invalid condition syntax: {condition}") from e
        except Exception as e:
            raise FilterEvaluationError(
                f"Unexpected error while evaluating '{condition}': {e}",
            ) from e

    @staticmethod
    def _preprocess(condition: str) -> str:
        """
        Предобработка строки: оборачивает dependency-ссылки в кавычки.

        Args:
            condition: Исходное выражение.

        Returns:
            str: Модифицированная строка условия.
        """
        return _DEP_PATTERN.sub(r'"\1"', condition)

    @staticmethod
    def _parse(processed: str) -> ast.Expression:
        """
        Безопасный парсинг строки в AST.

        Args:
            processed: Предобработанное выражение.

        Returns:
            ast.Expression: AST-дерево выражения.
        """
        return ast.parse(processed, mode="eval")

    @staticmethod
    def _validate_ast(tree: ast.AST) -> None:
        """
        Жёсткая валидация AST.

        Разрешены узлы:
            Expression, BoolOp(And/Or), UnaryOp(Not),
            Compare (с разрешёнными операторами),
            Constant, Name, Load.

        Args:
            tree: AST дерева выражения.

        Raises:
            FilterUnsupportedSyntaxError: При обнаружении недопустимой конструкции.
            FilterUnsupportedOperatorError: При использовании запрещённого оператора сравнения.
        """
        allowed_nodes: tuple[type, ...] = (
            ast.Expression,
            ast.BoolOp,
            ast.UnaryOp,
            ast.Compare,
            ast.Constant,
            ast.Name,
            ast.Load,
            ast.And,
            ast.Or,
            ast.Not,
            ast.Eq,
            ast.NotEq,
            ast.Lt,
            ast.LtE,
            ast.Gt,
            ast.GtE,
            ast.Is,
            ast.IsNot,
        )

        for node in ast.walk(tree):
            if isinstance(node, allowed_nodes):
                if isinstance(node, ast.BoolOp) and not isinstance(node.op, (ast.And, ast.Or)):
                    raise FilterUnsupportedSyntaxError(
                        f"Boolean operator '{type(node.op).__name__}' is not allowed",
                    )
                if isinstance(node, ast.UnaryOp) and not isinstance(node.op, ast.Not):
                    raise FilterUnsupportedSyntaxError(
                        f"Unary operator '{type(node.op).__name__}' is not allowed",
                    )
                if isinstance(node, ast.Compare):
                    for op in node.ops:
                        if type(op) not in _ALLOWED_OPERATORS:
                            raise FilterUnsupportedOperatorError(
                                f"Operator '{type(op).__name__}' is not allowed",
                            )
                continue
            raise FilterUnsupportedSyntaxError(
                f"Unsupported syntax: {ast.dump(node, include_attributes=False)}",
            )

    def _eval(self, node: ast.AST, *, steps: Mapping[int, Any], local: Mapping[str, Any]) -> Any:
        """
        Рекурсивная оценка AST.

        Args:
            node: Узел AST.
            steps: Данные шагов.
            local: Локальный контекст.

        Returns:
            Любое значение, полученное при вычислении выражения.

        Raises:
            FilterUnsupportedSyntaxError: Для неподдерживаемых узлов.
        """
        if isinstance(node, ast.BoolOp):
            return self._eval_boolop(node, steps=steps, local=local)
        if isinstance(node, ast.UnaryOp):
            return self._eval_unary(node, steps=steps, local=local)
        if isinstance(node, ast.Compare):
            return self._eval_compare(node, steps=steps, local=local)
        if isinstance(node, ast.Constant):
            return self._eval_constant(node, steps=steps, local=local)
        if isinstance(node, ast.Name):
            return self._eval_name(node)
        raise FilterUnsupportedSyntaxError(
            f"Unsupported syntax: {ast.dump(node, include_attributes=False)}",
        )

    def _eval_boolop(self, node: ast.BoolOp, *, steps: Mapping[int, Any], local: Mapping[str, Any]) -> bool:
        """
        Оценка булевых операций and/or.

        Args:
            node: Узел BoolOp.
            steps: Данные шагов.
            local: Локальный контекст.

        Returns:
            bool: Результат логической операции.
        """
        if isinstance(node.op, ast.And):
            return all(self._eval(v, steps=steps, local=local) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(self._eval(v, steps=steps, local=local) for v in node.values)
        raise FilterUnsupportedSyntaxError(f"Unsupported BoolOp: {node.op}")

    def _eval_unary(self, node: ast.UnaryOp, *, steps: Mapping[int, Any], local: Mapping[str, Any]) -> bool:
        """
        Оценка унарной операции not.

        Args:
            node: Узел UnaryOp.
            steps: Данные шагов.
            local: Локальный контекст.

        Returns:
            bool: Результат операции not.
        """
        if isinstance(node.op, ast.Not):
            return not self._eval(node.operand, steps=steps, local=local)
        raise FilterUnsupportedSyntaxError(f"Unsupported UnaryOp: {node.op}")

    def _eval_compare(self, node: ast.Compare, *, steps: Mapping[int, Any], local: Mapping[str, Any]) -> bool:
        """
        Оценка сравнений, включая цепочки (a < b < c).

        Args:
            node: Узел Compare.
            steps: Данные шагов.
            local: Локальный контекст.

        Returns:
            bool: Итог сравнения.
        """
        left = self._eval(node.left, steps=steps, local=local)
        for op, comp in zip(node.ops, node.comparators, strict=False):
            right = self._eval(comp, steps=steps, local=local)
            op_func = _ALLOWED_OPERATORS.get(type(op))
            if op_func is None:
                raise FilterUnsupportedOperatorError(f"Operator '{type(op).__name__}' is not allowed")
            if not op_func(left, right):
                return False
            left = right
        return True

    def _eval_constant(self, node: ast.Constant, *, steps: Mapping[int, Any], local: Mapping[str, Any]) -> Any:
        """
        Оценка констант.

        Для строк дополнительно поддерживает Dependency-ссылки.

        Args:
            node: Узел Constant.
            steps: Данные шагов.
            local: Локальный контекст.

        Returns:
            Любое значение.
        """
        if isinstance(node.value, str):
            try:
                dep = Dependency.parse(node.value)
            except DependencyError as e:
                raise FilterDependencyError(f"Invalid dependency: {e}") from e

            if dep:
                if dep.step_id is None:
                    data = local
                    if not isinstance(data, Mapping):
                        raise FilterDependencyError("Local context is not provided")
                else:
                    if dep.step_id not in steps:
                        raise FilterMissingStepError(f"Missing step {dep.step_id} in results")
                    data = steps[dep.step_id]

                return dep.resolve(data)
        return node.value

    @staticmethod
    def _eval_name(node: ast.Name) -> Any:
        """
        Оценка имён-литералов: True/False/None и их нижний регистр.

        Args:
            node: Узел Name.

        Returns:
            Любое значение (bool или None).

        Raises:
            FilterUnknownIdentifierError: Для неизвестных идентификаторов.
        """
        if node.id in {"True", "true"}:
            return True
        if node.id in {"False", "false"}:
            return False
        if node.id in {"None", "null"}:
            return None
        raise FilterUnknownIdentifierError(f"Unknown identifier: {node.id}")
