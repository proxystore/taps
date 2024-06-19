from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field

from taps.transformer.file import PickleFileTransformerConfig
from taps.transformer.null import NullTransformer
from taps.transformer.protocol import DataTransformer
from taps.transformer.proxy import ProxyTransformerConfig


class DataTransformerConfigs(BaseModel):
    """Data transformer choice configuration."""

    file: Optional[PickleFileTransformerConfig] = Field(None)  # noqa: UP007
    proxy: Optional[ProxyTransformerConfig] = Field(None)  # noqa: UP007

    def get_transformer(self, name: str | None = None) -> DataTransformer[Any]:
        if name is None:
            return NullTransformer()
        attr = name.replace('-', '_')
        config = getattr(self, attr)
        return config.get_transformer()
