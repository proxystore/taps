from __future__ import annotations

from taps.filter import NeverFilter
from taps.filter import NeverFilterConfig


def test_never_filter() -> None:
    filter_ = NeverFilter()
    assert not filter_(object())


def test_never_filter_config() -> None:
    config = NeverFilterConfig()
    assert isinstance(config.get_filter(), NeverFilter)
