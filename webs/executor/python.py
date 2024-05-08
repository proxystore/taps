from __future__ import annotations

import multiprocessing
from concurrent.futures import ThreadPoolExecutor

from pydantic import Field

from webs.executor.config import ExecutorConfig
from webs.executor.config import register
from webs.executor.dag import DAGExecutor


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
