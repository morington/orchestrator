import pytest

from orchestrator.core.entities import MiddlewareMeta
from orchestrator.core.errors import MiddlewareDependencyError
from orchestrator.core.middleware import MiddlewareExecutor


def test_set_simple_key():
    spec = MiddlewareMeta(set={"a": 1})
    result = MiddlewareExecutor(spec).run(data={})
    assert result == {"a": 1}


def test_set_nested_dot_path():
    spec = MiddlewareMeta(set={"a.b.c": "v"})
    result = MiddlewareExecutor(spec).run(data={})
    assert result == {"a": {"b": {"c": "v"}}}


def test_set_with_list_autocreate():
    spec = MiddlewareMeta(set={"items.0.id": 7})
    result = MiddlewareExecutor(spec).run(data={})
    assert result == {"items": [{"id": 7}]}


def test_set_resolves_global_dependency():
    spec = MiddlewareMeta(set={"x": "$1:value"})
    result = MiddlewareExecutor(spec).run(global_data={1: {"value": 99}}, data={})
    assert result == {"x": 99}


def test_remove_key():
    spec = MiddlewareMeta(remove=["a.b"])
    result = MiddlewareExecutor(spec).run(data={"a": {"b": 1, "c": 2}})
    assert result == {"a": {"c": 2}}


def test_input_is_not_mutated():
    original = {"a": 1}
    spec = MiddlewareMeta(set={"b": 2})
    result = MiddlewareExecutor(spec).run(data=original)
    assert original == {"a": 1}
    assert result == {"a": 1, "b": 2}


def test_missing_global_step_raises():
    spec = MiddlewareMeta(set={"x": "$5:value"})
    with pytest.raises(MiddlewareDependencyError):
        MiddlewareExecutor(spec).run(global_data={1: {"value": 1}}, data={})


def test_none_spec_returns_copy():
    result = MiddlewareExecutor(None).run(data={"a": 1})
    assert result == {"a": 1}
