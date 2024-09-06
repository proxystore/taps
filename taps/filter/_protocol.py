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
    """Filter protocol.

    A Filter is a callable object, e.g., a function, used by the
    [`Engine`][taps.engine.Engine] that takes an object as input and returns
    a boolean indicating if the object should be transformed by
    [`Engine`][taps.engine.Engine]'s data
    [`Transformer`][taps.transformer.Transformer].
    """

    def __call__(self, obj: Any) -> bool: ...


class FilterConfig(BaseModel, abc.ABC):
    """Abstract [`Filter`][taps.filter.Filter] plugin configuration."""

    name: str = Field(description='Filter name.')

    model_config: ConfigDict = ConfigDict(  # type: ignore[misc]
        extra='forbid',
        validate_default=True,
        validate_return=True,
    )

    @abc.abstractmethod
    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        ...
