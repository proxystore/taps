from __future__ import annotations

import abc
import math
from typing import Literal

from pydantic import BaseModel
from pydantic import Field

from taps import plugins
from taps.filter.filters import Filter
from taps.filter.filters import NullFilter
from taps.filter.filters import ObjectSizeFilter
from taps.filter.filters import PickleSizeFilter


class FilterConfig(BaseModel, abc.ABC):
    name: str

    @abc.abstractmethod
    def get_filter(self) -> Filter: ...


@plugins.register('filter')
class NullFilterConfig(FilterConfig):
    name: Literal['null'] = 'null'

    def get_filter(self) -> Filter:
        return NullFilter()


@plugins.register('filter')
class ObjectSizeConfig(FilterConfig):
    name: Literal['object-size'] = 'object-size'
    min_size: int = Field(0)
    max_size: float = Field(math.inf)

    def get_filter(self) -> Filter:
        return ObjectSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )


@plugins.register('filter')
class PickleSizeConfig(FilterConfig):
    name: Literal['pickle-size'] = 'pickle-size'
    min_size: int = Field(0)
    max_size: float = Field(math.inf)

    def get_filter(self) -> Filter:
        return PickleSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )
