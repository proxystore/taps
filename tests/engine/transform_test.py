from __future__ import annotations

import uuid
from typing import Any
from typing import TypeVar

from taps.data.filter import NullFilter
from taps.data.filter import ObjectTypeFilter
from taps.engine.transform import TaskDataTransformer

T = TypeVar('T')


class DictTransformer:
    def __init__(self) -> None:
        self.data: dict[uuid.UUID, Any] = {}

    def close(self) -> None:
        pass

    def is_identifier(self, obj: T) -> bool:
        return isinstance(obj, uuid.UUID)

    def transform(self, obj: T) -> uuid.UUID:
        key = uuid.uuid4()
        self.data[key] = obj
        return key

    def resolve(self, identifier: uuid.UUID) -> Any:
        return self.data.pop(identifier)


def test_task_data_transfomer() -> None:
    transformer = TaskDataTransformer(DictTransformer(), NullFilter())

    obj = object()
    identifier = transformer.transform(obj)
    assert obj != identifier
    assert transformer.resolve(identifier) == obj

    transformer.close()


def test_task_data_transfomer_iterable() -> None:
    transformer = TaskDataTransformer(DictTransformer(), NullFilter())

    objs = (object(), object())
    identifiers = transformer.transform_iterable(objs)
    assert objs != identifiers
    assert transformer.resolve_iterable(identifiers) == objs

    transformer.close()


def test_task_data_transfomer_mapping() -> None:
    transformer = TaskDataTransformer(DictTransformer(), NullFilter())

    objs = {'a': object(), 'b': object()}
    identifiers = transformer.transform_mapping(objs)
    assert objs != identifiers
    assert objs.keys() == identifiers.keys()
    assert transformer.resolve_mapping(identifiers) == objs

    transformer.close()


def test_task_data_transfomer_filter() -> None:
    transformer = TaskDataTransformer(DictTransformer(), ObjectTypeFilter(str))

    obj = object()
    identifier = transformer.transform(obj)
    assert identifier is obj

    obj = 'object'
    identifier = transformer.transform(obj)
    assert identifier is not obj
    assert transformer.resolve(identifier) == obj

    transformer.close()


def test_task_data_transfomer_iterable_filter() -> None:
    transformer = TaskDataTransformer(DictTransformer(), ObjectTypeFilter(str))

    objs = (object(), 'object')
    identifiers = transformer.transform_iterable(objs)
    assert objs != identifiers
    assert transformer.resolve_iterable(identifiers) == objs

    transformer.close()


def test_task_data_transfomer_mapping_filter() -> None:
    transformer = TaskDataTransformer(DictTransformer(), ObjectTypeFilter(str))

    objs = {'a': object(), 'b': 'object'}
    identifiers = transformer.transform_mapping(objs)
    assert objs != identifiers
    assert objs.keys() == identifiers.keys()
    assert transformer.resolve_mapping(identifiers) == objs

    transformer.close()
