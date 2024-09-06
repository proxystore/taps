from __future__ import annotations

from taps.filter import Filter
from taps.filter import ObjectSizeFilter
from taps.filter import ObjectSizeFilterConfig
from taps.filter import ObjectTypeFilter
from taps.filter import ObjectTypeFilterConfig
from taps.filter import PickleSizeFilter
from taps.filter import PickleSizeFilterConfig


def test_object_size_filter() -> None:
    filter_ = ObjectSizeFilter(min_bytes=32, max_bytes=100)
    assert isinstance(repr(filter_), str)

    assert not filter_(object())
    assert filter_('object')
    assert not filter_('x' * 100)


def test_object_type_filter_config() -> None:
    config = ObjectTypeFilterConfig(patterns=['bytes', 'str'])

    filter_ = config.get_filter()
    assert filter_(b'')
    assert filter_('')
    assert not filter_(42)


def test_object_type_filter() -> None:
    filter_ = ObjectTypeFilter(str, tuple)
    assert isinstance(repr(filter_), str)

    assert filter_('object')
    assert filter_(())
    assert not filter_([])


def test_object_type_filter_patterns() -> None:
    filter_ = ObjectTypeFilter(patterns=('bytes', 'Foo'))

    class Foo:
        pass

    class Foobar:
        pass

    assert filter_(b'')
    assert filter_(Foo())
    assert filter_(Foobar())
    assert not filter_(42)

    filter_ = ObjectTypeFilter(patterns=('Foo$',))
    assert filter_(Foo())
    assert not filter_(Foobar())


def test_object_size_filter_config() -> None:
    config = ObjectSizeFilterConfig(min_size=100, max_size=1000)

    filter_ = config.get_filter()
    assert isinstance(filter_, Filter)
    assert not filter_(b'')
    assert filter_(b'x' * 100)
    assert not filter_(b'x' * 1000)


def test_pickle_size_filter() -> None:
    filter_ = PickleSizeFilter(min_bytes=64, max_bytes=128)
    assert isinstance(repr(filter_), str)

    assert not filter_(object())
    assert filter_(b'x' * 80)
    assert not filter_(b'x' * 256)


def test_pickle_size_filter_config() -> None:
    config = PickleSizeFilterConfig(min_size=100, max_size=1000)

    filter_ = config.get_filter()
    assert isinstance(filter_, Filter)
    assert not filter_(b'')
    assert filter_(b'x' * 100)
    assert not filter_(b'x' * 1000)
