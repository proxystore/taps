from __future__ import annotations

import argparse
import math
from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional
from typing import Sequence

from pydantic import Field

from webs.config import Config
from webs.data.filter import Filter
from webs.data.filter import NullFilter
from webs.data.filter import ObjectSizeFilter
from webs.data.filter import PickleSizeFilter
from webs.data.transform import TransformerConfig


class FilterConfig(Config):
    """Filter configuration."""

    filter_type: Optional[Literal['object-size', 'pickle-size']] = Field(  # noqa: UP007
        None,
        description='filter type (object-size or pickle-size)',
    )
    filter_min_size: int = Field(
        0,
        description='min size for filter in bytes',
    )
    filter_max_size: float = Field(
        math.inf,
        description='max size for filter in bytes',
    )

    def get_filter(self) -> Filter:
        """Get a filter according to the config."""
        if self.filter_type is None:
            return NullFilter()
        elif self.filter_type == 'object-size':
            return ObjectSizeFilter(
                min_bytes=self.filter_min_size,
                max_bytes=self.filter_max_size,
            )
        elif self.filter_type == 'pickle-size':
            return PickleSizeFilter(
                min_bytes=self.filter_min_size,
                max_bytes=self.filter_max_size,
            )
        else:
            raise AssertionError(f'Unknown filter type {self.filter_type}.')


class TransformerChoicesConfig(Config):
    """Transformer choice configuration."""

    transformer: str

    @classmethod
    def add_argument_group(
        cls,
        parser: argparse.ArgumentParser,
        *,
        argv: Sequence[str] | None = None,
        required: bool = True,
    ) -> None:
        """Add model fields as arguments of an argument group on the parser.

        Args:
            parser: Parser to add a new argument group to.
            argv: Optional sequence of string arguments.
            required: Mark arguments without defaults as required.
        """
        configs = get_registered()

        group = parser.add_argument_group(cls.__name__)
        group.add_argument(
            '--transformer',
            choices=sorted(configs.keys()),
            default='null',
            help='executor to use',
        )

        executor_type: str | None = None
        if argv is not None and '--executor' in argv:  # pragma: no cover
            executor_type = argv[argv.index('--executor') + 1]

        for name, config_type in configs.items():
            config_type.add_argument_group(
                parser,
                argv=argv,
                required=name == executor_type,
            )


class _TransformerConfigRegistry:
    def __init__(self) -> None:
        self._configs: dict[str, type[TransformerConfig]] = {}

    def get_transformer_config(
        self,
        transformer: str,
        **options: Any,
    ) -> TransformerConfig:
        return self._configs[transformer](**options)

    def get_registered(self) -> dict[str, type[TransformerConfig]]:
        return self._configs

    def register(
        self,
        *,
        name: str,
    ) -> Callable[[type[TransformerConfig]], type[TransformerConfig]]:
        def decorator(cls: type[TransformerConfig]) -> type[TransformerConfig]:
            self._configs[name] = cls
            return cls

        return decorator


_transformer_configs = _TransformerConfigRegistry()
register = _transformer_configs.register
get_transformer_config = _transformer_configs.get_transformer_config
get_registered = _transformer_configs.get_registered
