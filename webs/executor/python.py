from __future__ import annotations

import multiprocessing
from concurrent.futures import ThreadPoolExecutor

from pydantic import Field

from webs.executor.config import ExecutorConfig
from webs.executor.config import register


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

    def get_executor(self) -> ThreadPoolExecutor:
        """Create an executor instance from the config."""
        return ThreadPoolExecutor(self.max_threads)
