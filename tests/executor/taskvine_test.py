from __future__ import annotations

import pathlib
from unittest import mock

import pytest

from taps.executor.taskvine import taskvine_import_error
from taps.executor.taskvine import TaskVineConfig
from taps.executor.taskvine import TaskVineExecutor
from taps.run.utils import change_cwd
from testing.utils import open_port


def test_taskvine_config() -> None:
    with mock.patch('taps.executor.taskvine.TaskVineExecutor'):
        config = TaskVineConfig()
        config.get_executor()

        config = TaskVineConfig(cores_per_worker=1)
        config.get_executor()


@pytest.mark.skipif(
    taskvine_import_error is not None,
    reason='taskvine is not installed',
)
def test_taskvine_executor_serverless(
    tmp_path: pathlib.Path,
) -> None:  # pragma: no cover
    with change_cwd(tmp_path):
        with TaskVineExecutor(
            manager_name='test-taskvine-executor-serverless',
            opts={'min_workers': 1, 'max-workers': 1, 'cores': 1},
            cores_per_task=1,
            factory=True,
            port=open_port(),
            serverless=True,
        ) as executor:
            future = executor.submit(abs, 1)
            assert future.result() == 1

            future = executor.submit(abs, -1)
            assert future.result() == 1

            results = executor.map(abs, [1, -1])
            assert list(results) == [1, 1]


@pytest.mark.skipif(
    taskvine_import_error is not None,
    reason='taskvine is not installed',
)
def test_taskvine_executor_non_serverless(
    tmp_path: pathlib.Path,
) -> None:  # pragma: no cover
    with change_cwd(tmp_path):
        with TaskVineExecutor(
            manager_name='test-taskvine-executor-non-serverless',
            opts={'min_workers': 1, 'max-workers': 1},
            cores_per_task=1,
            factory=True,
            port=open_port(),
            serverless=False,
        ) as executor:
            future = executor.submit(abs, 1)
            assert future.result() == 1

            future = executor.submit(abs, -1)
            assert future.result() == 1

            results = executor.map(abs, [1, -1])
            assert list(results) == [1, 1]
