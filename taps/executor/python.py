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
    """[`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor] plugin configuration.

    Attributes:
        max_processes: Maximum number of processes.
        context: Multiprocessing context type (fork, spawn, or forkserver).
    """  # noqa: E501

    name: Literal['process-pool'] = 'process-pool'
    max_processes: int = Field(
        multiprocessing.cpu_count(),
        description='maximum number of processes',
    )
    context: Optional[  # noqa: UP007
        Literal['fork', 'spawn', 'forkserver']
    ] = Field(
        None,
        description=(
            'multiprocessing start method (one of fork, spawn, or forkserver)'
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
    """[`ThreadPoolExecutor`][concurrent.futures.ThreadPoolExecutor] plugin configuration.

    Attributes:
        max_threads: Maximum number of threads.
    """  # noqa: E501

    name: Literal['thread-pool'] = 'thread-pool'
    max_threads: int = Field(
        multiprocessing.cpu_count(),
        description='maximum number of threads',
    )

    def get_executor(self) -> FutureDependencyExecutor:
        """Create an executor instance from the config."""
        return FutureDependencyExecutor(ThreadPoolExecutor(self.max_threads))
