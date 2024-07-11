from __future__ import annotations

import abc
import math
from typing import List
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from taps.filter.filters import AllFilter
from taps.filter.filters import Filter
from taps.filter.filters import NullFilter
from taps.filter.filters import ObjectSizeFilter
from taps.filter.filters import ObjectTypeFilter
from taps.filter.filters import PickleSizeFilter
from taps.plugins import register


class FilterConfig(BaseModel, abc.ABC):
    """Abstract filter configuration."""

    name: str = Field(description='name of filter type')

    model_config: ConfigDict = ConfigDict(  # type: ignore[misc]
        extra='ignore',
        validate_default=True,
        validate_return=True,
    )

    @abc.abstractmethod
    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        ...


@register('filter')
class AllFilterConfig(FilterConfig):
    """All filter configuration."""

    name: Literal['all'] = Field('all', description='name of filter type')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return AllFilter()


@register('filter')
class NullFilterConfig(FilterConfig):
    """Null filter configuration."""

    name: Literal['null'] = Field('null', description='name of filter type')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return NullFilter()


@register('filter')
class ObjectSizeConfig(FilterConfig):
    """Object size filter configuration."""

    name: Literal['object-size'] = Field(
        'object-size',
        description='name of filter type',
    )
    min_size: int = Field(0, description='minimum object size in bytes')
    max_size: float = Field(
        math.inf,
        description='maximum object size in bytes',
    )

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return ObjectSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )


@register('filter')
class ObjectTypeConfig(FilterConfig):
    """Object type filter configuration."""

    name: Literal['object-type'] = Field(
        'object-type',
        description='name of filter type',
    )
    patterns: Optional[List[str]] = Field(  # noqa: UP006,UP007
        None,
        description='list of patterns to match against type names',
    )

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return ObjectTypeFilter(patterns=self.patterns)


@register('filter')
class PickleSizeConfig(FilterConfig):
    """Pickled object size filter configuration."""

    name: Literal['pickle-size'] = Field(
        'pickle-size',
        description='name of filter type',
    )
    min_size: int = Field(0, description='minimum object size in bytes')
    max_size: float = Field(
        math.inf,
        description='maximum object size in bytes',
    )

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return PickleSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )
