from __future__ import annotations

import abc
from typing import Any
from typing import Protocol
from typing import runtime_checkable

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


@runtime_checkable
class Filter(Protocol):
    """Filter protocol."""

    def __call__(self, obj: Any) -> bool:
        """Check if an abject passes through the filter."""
        ...


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
