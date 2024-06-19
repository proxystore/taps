from __future__ import annotations

import math

from pydantic import BaseModel
from pydantic import Field

from taps.filter.filters import Filter
from taps.filter.filters import NullFilter
from taps.filter.filters import ObjectSizeFilter
from taps.filter.filters import PickleSizeFilter


class ObjectSizeConfig(BaseModel):
    min_size: int = Field(0)
    max_size: float = Field(math.inf)

    def get_filter(self) -> Filter:
        return ObjectSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )


class PickleSizeConfig(BaseModel):
    min_size: int = Field(0)
    max_size: float = Field(math.inf)

    def get_filter(self) -> Filter:
        return PickleSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )


class FilterConfigs(BaseModel):
    # TODO: add type
    object_size: ObjectSizeConfig = Field(default_factory=ObjectSizeConfig)
    pickle_size: PickleSizeConfig = Field(default_factory=PickleSizeConfig)

    def get_filter(self, name: str | None = None) -> Filter:
        if name is None:
            return NullFilter()
        attr = name.replace('-', '_')
        config = getattr(self, attr)
        return config.get_filter()
