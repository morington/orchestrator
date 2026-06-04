import pytest

from orchestrator.core.dependency import Dependency
from orchestrator.core.errors import (
    DependencyFormatError,
    IndexOutOfRangeError,
    KeyNotFoundError,
    TypeMismatchError,
)


def test_parse_global_ref():
    dep = Dependency.parse("$1:result.value")
    assert dep is not None
    assert dep.step_id == 1
    assert dep.path == ("result", "value")


def test_parse_local_ref():
    dep = Dependency.parse("$foo.bar")
    assert dep is not None
    assert dep.step_id is None
    assert dep.path == ("foo", "bar")


def test_parse_numeric_segment_becomes_int():
    dep = Dependency.parse("$1:items.0")
    assert dep is not None
    assert dep.path == ("items", 0)


def test_parse_non_dollar_returns_none():
    assert Dependency.parse("plain") is None


def test_parse_invalid_format_raises():
    with pytest.raises(DependencyFormatError):
        Dependency.parse("$1:")


def test_resolve_nested_dict():
    dep = Dependency.parse("$1:a.b")
    assert dep.resolve({"a": {"b": 42}}) == 42


def test_resolve_list_index():
    dep = Dependency.parse("$1:items.1")
    assert dep.resolve({"items": ["x", "y"]}) == "y"


def test_resolve_index_out_of_range():
    dep = Dependency.parse("$1:items.5")
    with pytest.raises(IndexOutOfRangeError):
        dep.resolve({"items": ["x"]})


def test_resolve_missing_key():
    dep = Dependency.parse("$1:missing")
    with pytest.raises(KeyNotFoundError):
        dep.resolve({"present": 1})


def test_resolve_type_mismatch():
    dep = Dependency.parse("$1:a.b")
    with pytest.raises(TypeMismatchError):
        dep.resolve({"a": 5})


def test_resolve_dict_round_trip():
    resolved = Dependency.resolve_dict(
        data={"x": "$1:value", "y": "literal", "nested": {"z": "$foo"}},
        local_results={"foo": "local"},
        global_results={1: {"value": 99}},
    )
    assert resolved == {"x": 99, "y": "literal", "nested": {"z": "local"}}


def test_dependency_str_round_trip():
    assert str(Dependency.parse("$2:a.b.0")) == "$2:a.b.0"
