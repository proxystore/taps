from __future__ import annotations

import uuid
from typing import Any
from typing import TypeVar

from taps.engine.transform import TaskTransformer
from taps.filter import ObjectTypeFilter

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
    with TaskTransformer(DictTransformer()) as transformer:
        assert isinstance(repr(transformer), str)

        obj = object()
        identifier = transformer.transform(obj)
        assert obj != identifier
        assert transformer.resolve(identifier) == obj


def test_task_data_transfomer_defaults() -> None:
    with TaskTransformer() as transformer:
        assert isinstance(repr(transformer), str)

        obj = object()
        identifier = transformer.transform(obj)
        assert obj is identifier
        assert transformer.resolve(identifier) is obj


def test_task_data_transfomer_iterable() -> None:
    with TaskTransformer(DictTransformer()) as transformer:
        objs = (object(), object())
        identifiers = transformer.transform_iterable(objs)
        assert objs != identifiers
        assert transformer.resolve_iterable(identifiers) == objs


def test_task_data_transfomer_mapping() -> None:
    with TaskTransformer(DictTransformer()) as transformer:
        objs = {'a': object(), 'b': object()}
        identifiers = transformer.transform_mapping(objs)
        assert objs != identifiers
        assert objs.keys() == identifiers.keys()
        assert transformer.resolve_mapping(identifiers) == objs


def test_task_data_transfomer_filter() -> None:
    with TaskTransformer(
        DictTransformer(),
        ObjectTypeFilter(str),
    ) as transformer:
        obj = object()
        identifier = transformer.transform(obj)
        assert identifier is obj

        obj = 'object'
        identifier = transformer.transform(obj)
        assert identifier is not obj
        assert transformer.resolve(identifier) == obj


def test_task_data_transfomer_iterable_filter() -> None:
    with TaskTransformer(
        DictTransformer(),
        ObjectTypeFilter(str),
    ) as transformer:
        objs = (object(), 'object')
        identifiers = transformer.transform_iterable(objs)
        assert objs != identifiers
        assert transformer.resolve_iterable(identifiers) == objs


def test_task_data_transfomer_mapping_filter() -> None:
    with TaskTransformer(
        DictTransformer(),
        ObjectTypeFilter(str),
    ) as transformer:
        objs = {'a': object(), 'b': 'object'}
        identifiers = transformer.transform_mapping(objs)
        assert objs != identifiers
        assert objs.keys() == identifiers.keys()
        assert transformer.resolve_mapping(identifiers) == objs
