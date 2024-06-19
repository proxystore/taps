from __future__ import annotations

from concurrent.futures import Executor

import pytest

from taps.executor.config import ExecutorConfigs


def test_get_executor_from_config() -> None:
    config = ExecutorConfigs()

    assert isinstance(config.get_executor('thread_pool'), Executor)
    assert isinstance(config.get_executor('thread-pool'), Executor)


def test_get_executor_from_config_error() -> None:
    config = ExecutorConfigs()

    with pytest.raises(ValueError, match='No executor named missing.'):
        config.get_executor('missing')
