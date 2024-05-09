from __future__ import annotations

from webs.executor.dag import DAGExecutor
from webs.executor.python import ProcessPoolConfig
from webs.executor.python import ThreadPoolConfig


def test_thread_pool_config() -> None:
    config = ThreadPoolConfig(max_threads=1)
    with config.get_executor() as executor:
        assert isinstance(executor, DAGExecutor)


def test_process_pool_config() -> None:
    config = ProcessPoolConfig(max_threads=1)
    with config.get_executor() as executor:
        assert isinstance(executor, DAGExecutor)
