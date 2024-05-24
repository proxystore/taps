from __future__ import annotations

import uuid
from typing import Any
from typing import TypeVar

import pytest

from webs.data.filter import ObjectTypeFilter
from webs.data.null import NullTransformer
from webs.data.transform import TaskDataTransformer

T = TypeVar('T')


class DictTransformer:
    def __init__(self) -> None:
        self.data: dict[uuid.UUID, Any] = {}

    def is_identifier(self, obj: T) -> bool:
        return isinstance(obj, uuid.UUID)

    def transform(self, obj: T) -> uuid.UUID:
        key = uuid.uuid4()
        self.data[key] = obj
        return key

    def resolve(self, identifier: uuid.UUID) -> Any:
        return self.data.pop(identifier)


def test_null_transformer() -> None:
    transformer = NullTransformer()
    obj = object()
    assert not transformer.is_identifier(obj)
    assert transformer.transform(obj) is obj
    with pytest.raises(NotImplementedError):
        transformer.resolve(obj)


def test_task_data_transfomer() -> None:
    transformer = TaskDataTransformer(DictTransformer())

    obj = object()
    identifier = transformer.transform(obj)
    assert obj != identifier
    assert transformer.resolve(identifier) == obj


def test_task_data_transfomer_iterable() -> None:
    transformer = TaskDataTransformer(DictTransformer())

    objs = (object(), object())
    identifiers = transformer.transform_iterable(objs)
    assert objs != identifiers
    assert transformer.resolve_iterable(identifiers) == objs


def test_task_data_transfomer_mapping() -> None:
    transformer = TaskDataTransformer(DictTransformer())

    objs = {'a': object(), 'b': object()}
    identifiers = transformer.transform_mapping(objs)
    assert objs != identifiers
    assert objs.keys() == identifiers.keys()
    assert transformer.resolve_mapping(identifiers) == objs


def test_task_data_transfomer_filter() -> None:
    transformer = TaskDataTransformer(DictTransformer(), ObjectTypeFilter(str))

    obj = object()
    identifier = transformer.transform(obj)
    assert identifier is obj

    obj = 'object'
    identifier = transformer.transform(obj)
    assert identifier is not obj
    assert transformer.resolve(identifier) == obj


def test_task_data_transfomer_iterable_filter() -> None:
    transformer = TaskDataTransformer(DictTransformer(), ObjectTypeFilter(str))

    objs = (object(), 'object')
    identifiers = transformer.transform_iterable(objs)
    assert objs != identifiers
    assert transformer.resolve_iterable(identifiers) == objs


def test_task_data_transfomer_mapping_filter() -> None:
    transformer = TaskDataTransformer(DictTransformer(), ObjectTypeFilter(str))

    objs = {'a': object(), 'b': 'object'}
    identifiers = transformer.transform_mapping(objs)
    assert objs != identifiers
    assert objs.keys() == identifiers.keys()
    assert transformer.resolve_mapping(identifiers) == objs
