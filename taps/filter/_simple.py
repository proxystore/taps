from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import Field

from taps.filter._protocol import Filter
from taps.filter._protocol import FilterConfig
from taps.plugins import register


class AllFilter:
    """Filter that lets all objects pass through.

    ```python
    from taps.filter import AllFilter

    filter_ = AllFilter()
    assert filter_('value')  # always true
    ```
    """

    def __call__(self, obj: Any) -> bool:
        return True


@register('filter')
class AllFilterConfig(FilterConfig):
    """[`AllFilter`][taps.filter.AllFilter] plugin configuration."""

    name: Literal['all'] = Field('all', description='name of filter type')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return AllFilter()


class NullFilter:
    """Filter that lets no objects pass through.

    ```python
    from taps.filter import NullFilter

    filter_ = NullFilter()
    assert not filter_('value')  # always false
    ```
    """

    def __call__(self, obj: Any) -> bool:
        return False


@register('filter')
class NullFilterConfig(FilterConfig):
    """[`NullFilter`][taps.filter.NullFilter] plugin configuration."""

    name: Literal['null'] = Field('null', description='name of filter type')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return NullFilter()
