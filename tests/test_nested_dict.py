from icebergdiag.utils import NestedDictAccessor
import pytest


def test_initialization():
    data = {"a": 1, "b": {"c": 2}}
    accessor = NestedDictAccessor(data)
    assert accessor.data == data


def test_successful_access():
    data = {"a": {"b": {"c": 123}}}
    accessor = NestedDictAccessor(data)
    assert accessor["a.b.c"] == 123


def test_key_error():
    data = {"x": {"y": 10}}
    accessor = NestedDictAccessor(data)
    with pytest.raises(KeyError):
        _ = accessor["a.b.c"]


def test_empty_key():
    data = {"a": 1, "b": {"c": 2}}
    accessor = NestedDictAccessor(data)
    with pytest.raises(KeyError):
        _ = accessor[""]


def test_empty_dict():
    data = {}
    accessor = NestedDictAccessor(data)
    with pytest.raises(KeyError):
        _ = accessor["a"]


def test_non_string_key():
    data = {"a": 1, "b": {"c": 2}}
    accessor = NestedDictAccessor(data)
    with pytest.raises(TypeError):
        _ = accessor[123]
