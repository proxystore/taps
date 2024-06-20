from __future__ import annotations

import abc
from typing import Any

from pydantic import BaseModel

from taps.transformer.protocol import DataTransformer


class DataTransformerConfig(BaseModel, abc.ABC):
    """Data transformer configuration."""

    name: str

    @abc.abstractmethod
    def get_transformer(self) -> DataTransformer[Any]:
        pass
