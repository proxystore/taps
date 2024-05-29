from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Literal
from typing import Optional

from pydantic import Field

from webs.executor.config import ExecutorConfig
from webs.executor.config import register
from webs.executor.dag import DAGExecutor


@register(name='process-pool')
class ProcessPoolConfig(ExecutorConfig):
    """Process pool executor configuration.

    Attributes:
        max_processes: Maximum number of processes.
    """

    max_processes: int = Field(
        multiprocessing.cpu_count(),
        description='maximum number of processes',
    )
    multiprocessing_context: Optional[  # noqa: UP007
        Literal['fork', 'spawn', 'forkserver']
    ] = Field(
        None,
        description=(
            'multiprocessing start method (one of fork, spawn, or forkserver)'
        ),
    )

    def get_executor(self) -> DAGExecutor:
        """Create an executor instance from the config."""
        context = multiprocessing.get_context(self.multiprocessing_context)
        return DAGExecutor(
            ProcessPoolExecutor(self.max_processes, mp_context=context),
        )


@register(name='thread-pool')
class ThreadPoolConfig(ExecutorConfig):
    """Thread pool executor configuration.

    Attributes:
        max_threads: Maximum number of threads.
    """

    max_threads: int = Field(
        multiprocessing.cpu_count(),
        description='maximum number of threads',
    )

    def get_executor(self) -> DAGExecutor:
        """Create an executor instance from the config."""
        return DAGExecutor(ThreadPoolExecutor(self.max_threads))
