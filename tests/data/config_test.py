from __future__ import annotations

from webs.data.config import FilterConfig


def test_filter_config() -> None:
    FilterConfig().get_filter()

    config = FilterConfig(filter_type='object-size')
    config.get_filter()

    config = FilterConfig(filter_type='pickle-size')
    config.get_filter()
