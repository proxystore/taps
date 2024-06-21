from __future__ import annotations

import abc
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from taps.transformer.protocol import DataTransformer


class DataTransformerConfig(BaseModel, abc.ABC):
    """Abstract transformer configuration."""

    name: str = Field(description='name of transformer type')

    model_config: ConfigDict = ConfigDict(  # type: ignore[misc]
        extra='forbid',
        validate_default=True,
        validate_return=True,
    )

    @abc.abstractmethod
    def get_transformer(self) -> DataTransformer[Any]:
        """Create a transformer from the configuration."""
        ...
