import pytest

from orchestrator.core.errors import (
    FilterMissingStepError,
    FilterUnknownIdentifierError,
    FilterUnsupportedSyntaxError,
)
from orchestrator.core.filter import ConditionEvaluator


@pytest.fixture
def evaluator() -> ConditionEvaluator:
    return ConditionEvaluator()


def test_literal_comparison(evaluator):
    assert evaluator.evaluate("1 < 2") is True
    assert evaluator.evaluate("2 == 3") is False


def test_global_ref_comparison(evaluator):
    assert evaluator.evaluate("$1:result == 42", global_data={1: {"result": 42}}) is True


def test_local_ref_comparison(evaluator):
    assert evaluator.evaluate("$flag == true", local={"flag": True}) is True


def test_chained_comparison(evaluator):
    assert evaluator.evaluate("1 < 2 < 3") is True


def test_boolean_logic(evaluator):
    assert evaluator.evaluate("true and not false") is True
    assert evaluator.evaluate("false or $1:ok", global_data={1: {"ok": True}}) is True


def test_none_literal(evaluator):
    assert evaluator.evaluate("$1:val is null", global_data={1: {"val": None}}) is True


def test_missing_step_raises(evaluator):
    with pytest.raises(FilterMissingStepError):
        evaluator.evaluate("$9:x == 1", global_data={1: {"x": 1}})


def test_unknown_identifier_raises(evaluator):
    with pytest.raises(FilterUnknownIdentifierError):
        evaluator.evaluate("foo == 1")


def test_forbidden_call_raises(evaluator):
    with pytest.raises(FilterUnsupportedSyntaxError):
        evaluator.evaluate("len('a') == 1")


def test_non_boolean_result_raises(evaluator):
    with pytest.raises(FilterUnsupportedSyntaxError):
        evaluator.evaluate("$1:x", global_data={1: {"x": 5}})
