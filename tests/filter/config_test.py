from __future__ import annotations

from taps.filter.config import AllFilterConfig
from taps.filter.config import NullFilterConfig
from taps.filter.config import ObjectSizeConfig
from taps.filter.config import PickleSizeConfig
from taps.filter.filters import AllFilter
from taps.filter.filters import Filter
from taps.filter.filters import NullFilter


def test_all_filter_config() -> None:
    config = AllFilterConfig()
    assert isinstance(config.get_filter(), AllFilter)


def test_null_filter_config() -> None:
    config = NullFilterConfig()
    assert isinstance(config.get_filter(), NullFilter)


def test_object_size_filter() -> None:
    config = ObjectSizeConfig(min_size=100, max_size=1000)

    filter_ = config.get_filter()
    assert isinstance(filter_, Filter)
    assert not filter_(b'')
    assert filter_(b'x' * 100)
    assert not filter_(b'x' * 1000)


def test_pickle_size_filter() -> None:
    config = PickleSizeConfig(min_size=100, max_size=1000)

    filter_ = config.get_filter()
    assert isinstance(filter_, Filter)
    assert not filter_(b'')
    assert filter_(b'x' * 100)
    assert not filter_(b'x' * 1000)
