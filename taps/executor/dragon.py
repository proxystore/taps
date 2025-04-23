from __future__ import annotations

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Literal

try:
    import dragon  # noqa: F401

    dragon_import_error: Exception | None = None
    os.environ['DRAGON_PATCH_MP'] = '1'
except ImportError as e:  # pragma: no cover
    dragon_import_error = e

from pydantic import Field

from taps.executor import ExecutorConfig
from taps.executor.utils import FutureDependencyExecutor
from taps.plugins import register


@register('executor')
class DragonConfig(ExecutorConfig):
    """Dragon HPC plugin configuration.

    Learn more about Dragon at
    [dragonhpc.github.io/dragon](https://dragonhpc.github.io/dragon){target=_blank}.

    Note:
        TaPS currently only supports single-node dragon deployments.
    """

    name: Literal['dragon'] = Field('dragon', description='Executor name.')
    max_processes: int = Field(
        multiprocessing.cpu_count(),
        description='Maximum number of processes.',
    )

    def get_executor(self) -> FutureDependencyExecutor:  # pragma: linux cover
        """Create an executor instance from the config."""
        if dragon_import_error is not None:  # pragma: no cover
            raise dragon_import_error

        multiprocessing.set_start_method('dragon')
        return FutureDependencyExecutor(
            ProcessPoolExecutor(self.max_processes),
        )
