from __future__ import annotations

import abc
import math
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from taps import plugins
from taps.filter.filters import Filter
from taps.filter.filters import NullFilter
from taps.filter.filters import ObjectSizeFilter
from taps.filter.filters import PickleSizeFilter


class FilterConfig(BaseModel, abc.ABC):
    """Abstract filter configuration."""

    name: str = Field(description='name of filter type')

    model_config: ConfigDict = ConfigDict(  # type: ignore[misc]
        extra='forbid',
        validate_default=True,
        validate_return=True,
    )

    @abc.abstractmethod
    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        ...


@plugins.register('filter')
class NullFilterConfig(FilterConfig):
    """Null filter configuration."""

    name: Literal['null'] = Field('null', description='name of filter type')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return NullFilter()


@plugins.register('filter')
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


@plugins.register('filter')
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
