from __future__ import annotations

from taps.filter.simple import AllFilter
from taps.filter.simple import AllFilterConfig
from taps.filter.simple import NullFilter
from taps.filter.simple import NullFilterConfig


def test_all_filter() -> None:
    filter_ = AllFilter()
    assert filter_(object())


def test_all_filter_config() -> None:
    config = AllFilterConfig()
    assert isinstance(config.get_filter(), AllFilter)


def test_null_filter() -> None:
    filter_ = NullFilter()
    assert not filter_(object())


def test_null_filter_config() -> None:
    config = NullFilterConfig()
    assert isinstance(config.get_filter(), NullFilter)
