from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Literal
from typing import Optional

from pydantic import Field

from taps.executor import ExecutorConfig
from taps.executor.utils import FutureDependencyExecutor
from taps.plugins import register


@register('executor')
class ProcessPoolConfig(ExecutorConfig):
    """[`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor] plugin configuration."""  # noqa: E501

    name: Literal['process-pool'] = Field(
        'process-pool',
        description='Executor name.',
    )
    max_processes: int = Field(
        multiprocessing.cpu_count(),
        description='Maximum number of processes.',
    )
    context: Optional[  # noqa: UP007
        Literal['fork', 'spawn', 'forkserver']
    ] = Field(
        None,
        description=(
            'Multiprocessing start method (one of fork, spawn, or forkserver).'
        ),
    )

    def get_executor(self) -> FutureDependencyExecutor:
        """Create an executor instance from the config."""
        context = multiprocessing.get_context(self.context)
        return FutureDependencyExecutor(
            ProcessPoolExecutor(self.max_processes, mp_context=context),
        )


@register('executor')
class ThreadPoolConfig(ExecutorConfig):
    """[`ThreadPoolExecutor`][concurrent.futures.ThreadPoolExecutor] plugin configuration."""  # noqa: E501

    name: Literal['thread-pool'] = Field(
        'thread-pool',
        description='Executor name.',
    )
    max_threads: int = Field(
        multiprocessing.cpu_count(),
        description='Maximum number of threads.',
    )

    def get_executor(self) -> FutureDependencyExecutor:
        """Create an executor instance from the config."""
        return FutureDependencyExecutor(ThreadPoolExecutor(self.max_threads))
