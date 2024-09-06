from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import Field

from taps.filter._protocol import Filter
from taps.filter._protocol import FilterConfig
from taps.plugins import register


class NeverFilter:
    """Filter that never lets objects pass through.

    ```python
    from taps.filter import NeverFilter

    filter_ = NeverFilter()
    assert not filter_('value')  # always false
    ```
    """

    def __call__(self, obj: Any) -> bool:
        return False


@register('filter')
class NeverFilterConfig(FilterConfig):
    """[`NeverFilter`][taps.filter.NeverFilter] plugin configuration."""

    name: Literal['never'] = Field('never', description='Filter name.')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return NeverFilter()
