from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import Field

from taps.filter.protocol import Filter
from taps.filter.protocol import FilterConfig
from taps.plugins import register


class AllFilter:
    """All filter that lets all objects pass through."""

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        return True


@register('filter')
class AllFilterConfig(FilterConfig):
    """All filter configuration."""

    name: Literal['all'] = Field('all', description='name of filter type')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return AllFilter()


class NullFilter:
    """Null filter that lets no objects pass through."""

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        return False


@register('filter')
class NullFilterConfig(FilterConfig):
    """Null filter configuration."""

    name: Literal['null'] = Field('null', description='name of filter type')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return NullFilter()
