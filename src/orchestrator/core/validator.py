from typing import Any

from orchestrator.core.dependency import Dependency
from orchestrator.core.entities import (
    PIPELINE_VERSION_1_0,
    GotoFilterAction,
    StepEntity,
    WorkflowDefinition,
)
from orchestrator.core.errors import DependencyError, WorkflowDefinitionError

SUPPORTED_PIPELINE_VERSIONS: frozenset[str] = frozenset({PIPELINE_VERSION_1_0})


class WorkflowDefinitionValidator:
    """
    Проверяет публичное определение workflow перед компиляцией в граф.

    Проверки: поддерживаемая версия DSL, дубли step_id, висящие depends_on,
    циклы в DAG, корректность GOTO targets, разрешимость $ref в data/outputs,
    запрещённые в 1.0 конструкции. Накапливает все ошибки и поднимает одну
    WorkflowDefinitionError со списком проблем.
    """

    def validate(self, definition: WorkflowDefinition) -> None:
        """
        Провалидировать определение.

        Args:
            definition: Публичное определение workflow.

        Raises:
            WorkflowDefinitionError: Если найдена хотя бы одна проблема.
        """
        issues: list[str] = []

        if definition.pipeline_version not in SUPPORTED_PIPELINE_VERSIONS:
            issues.append(
                f"unsupported pipeline_version '{definition.pipeline_version}'; "
                f"supported: {sorted(SUPPORTED_PIPELINE_VERSIONS)}",
            )

        step_ids = [step.step_id for step in definition.steps]
        known_ids = set(step_ids)
        self._check_duplicate_ids(step_ids, issues)
        self._check_dependencies(definition.steps, known_ids, issues)
        self._check_goto_targets(definition.steps, known_ids, issues)
        self._check_data_refs(definition.steps, known_ids, issues)
        self._check_output_refs(definition, known_ids, issues)
        self._check_cycles(definition.steps, issues)

        if issues:
            raise WorkflowDefinitionError("; ".join(issues))

    @staticmethod
    def _check_duplicate_ids(step_ids: list[int], issues: list[str]) -> None:
        seen: set[int] = set()
        for step_id in step_ids:
            if step_id in seen:
                issues.append(f"duplicate step_id {step_id}")
            seen.add(step_id)

    @staticmethod
    def _check_dependencies(steps: list[StepEntity], known_ids: set[int], issues: list[str]) -> None:
        for step in steps:
            for dep in step.depends_on or []:
                if dep.node == step.step_id:
                    issues.append(f"step {step.step_id} depends on itself")
                if dep.node not in known_ids:
                    issues.append(f"step {step.step_id} depends on unknown step {dep.node}")

    @staticmethod
    def _check_goto_targets(steps: list[StepEntity], known_ids: set[int], issues: list[str]) -> None:
        for step in steps:
            for flt in step.filters or []:
                if isinstance(flt, GotoFilterAction):
                    issues.extend(
                        f"step {step.step_id} GOTO to unknown step {target}"
                        for target in flt.targets
                        if target not in known_ids
                    )

    def _check_data_refs(self, steps: list[StepEntity], known_ids: set[int], issues: list[str]) -> None:
        for step in steps:
            issues.extend(
                f"step {step.step_id} data references unknown step {ref}"
                for ref in self._collect_refs(step.data)
                if ref not in known_ids
            )

    def _check_output_refs(
        self, definition: WorkflowDefinition, known_ids: set[int], issues: list[str],
    ) -> None:
        for name, spec in definition.outputs.items():
            try:
                dep = Dependency.parse(spec.ref)
            except DependencyError as e:
                issues.append(f"output '{name}' has invalid ref '{spec.ref}': {e}")
                continue
            if dep is None or dep.step_id is None:
                issues.append(f"output '{name}' must reference a step result '$node:path'")
            elif dep.step_id not in known_ids:
                issues.append(f"output '{name}' references unknown step {dep.step_id}")

    def _collect_refs(self, value: Any) -> set[int]:
        refs: set[int] = set()
        if isinstance(value, str):
            try:
                dep = Dependency.parse(value)
            except DependencyError:
                return refs
            if dep is not None and dep.step_id is not None:
                refs.add(dep.step_id)
        elif isinstance(value, dict):
            for item in value.values():
                refs |= self._collect_refs(item)
        elif isinstance(value, list):
            for item in value:
                refs |= self._collect_refs(item)
        return refs

    @staticmethod
    def _check_cycles(steps: list[StepEntity], issues: list[str]) -> None:
        # Рёбра depends_on: upstream -> downstream. Цикл DAG = ошибка.
        adjacency: dict[int, list[int]] = {step.step_id: [] for step in steps}
        for step in steps:
            for dep in step.depends_on or []:
                if dep.node in adjacency:
                    adjacency[dep.node].append(step.step_id)

        white = set(adjacency)
        gray: set[int] = set()
        black: set[int] = set()

        def _dfs(node: int) -> bool:
            white.discard(node)
            gray.add(node)
            for nxt in adjacency[node]:
                if nxt in black:
                    continue
                if nxt in gray or _dfs(nxt):
                    return True
            gray.discard(node)
            black.add(node)
            return False

        for node in list(adjacency):
            if node in white and _dfs(node):
                issues.append("cycle detected in depends_on graph")
                return
