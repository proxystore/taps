from __future__ import annotations

import abc
import argparse
import sys
from concurrent.futures import Executor
from typing import Any
from typing import Callable
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

from webs.config import Config


class ExecutorConfig(Config, abc.ABC):
    """Executor configuration abstract base class."""

    @abc.abstractmethod
    def get_executor(self) -> Executor:
        """Create an executor instance from the config."""
        ...


class ExecutorChoicesConfig(Config):
    """Executor choice configuration."""

    executor: str

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
            '--executor',
            choices=sorted(configs.keys()),
            required=required,
            help='executor to use',
        )

        executor_type: str | None = None
        if argv is not None and '--executor' in argv:
            executor_type = argv[argv.index('--executor') + 1]

        for name, config_type in configs.items():
            config_type.add_argument_group(
                parser,
                argv=argv,
                required=name == executor_type,
            )


class _ExecutorConfigRegistry:
    def __init__(self) -> None:
        self._configs: dict[str, type[ExecutorConfig]] = {}

    def get_executor_config(
        self,
        executor: str,
        **options: Any,
    ) -> ExecutorConfig:
        return self._configs[executor](**options)

    def get_registered(self) -> dict[str, type[ExecutorConfig]]:
        return self._configs

    def register(
        self,
        *,
        name: str,
    ) -> Callable[[type[ExecutorConfig]], type[ExecutorConfig]]:
        def decorator(cls: type[ExecutorConfig]) -> type[ExecutorConfig]:
            self._configs[name] = cls
            return cls

        return decorator


_executor_configs = _ExecutorConfigRegistry()
register = _executor_configs.register
get_executor_config = _executor_configs.get_executor_config
get_registered = _executor_configs.get_registered
