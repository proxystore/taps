from __future__ import annotations

import math
import pathlib
import time
from unittest import mock

import pytest

from taps.apps.synthetic import noop_task
from taps.apps.synthetic import randbytes
from taps.apps.synthetic import run_bag_of_tasks
from taps.apps.synthetic import run_diamond
from taps.apps.synthetic import run_reduce
from taps.apps.synthetic import run_sequential
from taps.apps.synthetic import SyntheticApp
from taps.apps.synthetic import WorkflowStructure
from taps.engine import AppEngine


@pytest.mark.parametrize('size', (0, 1, 10, 100))
def test_randbytes(size: int) -> None:
    b = randbytes(size)
    assert isinstance(b, bytes)
    assert len(b) == size


def test_noop_task() -> None:
    output_size = 100
    sleep = 0.001

    start = time.perf_counter()
    result = noop_task(b'data', output_size=output_size, sleep=sleep)
    runtime = time.perf_counter() - start

    assert sleep <= runtime
    assert len(result) == output_size


def test_synthetic_app(
    app_engine: AppEngine,
    tmp_path: pathlib.Path,
) -> None:
    kinds = {
        WorkflowStructure.BAG: run_bag_of_tasks,
        WorkflowStructure.DIAMOND: run_diamond,
        WorkflowStructure.REDUCE: run_reduce,
        WorkflowStructure.SEQUENTIAL: run_sequential,
    }

    for i, (kind, function) in enumerate(kinds.items()):
        app = SyntheticApp(
            structure=kind,
            task_count=3,
            task_data_bytes=100,
            task_sleep=0.001,
            bag_max_running=3,
            warmup_task=i % 2 == 0,
        )

        with mock.patch(
            f'taps.apps.synthetic.{function.__name__}',
        ) as mocked:
            app.run(app_engine, tmp_path)
            mocked.assert_called_once()


def test_run_bag_of_tasks(app_engine: AppEngine) -> None:
    task_count, task_sleep, max_running_tasks = 6, 0.001, 3

    start = time.perf_counter()
    run_bag_of_tasks(
        app_engine,
        task_count,
        0,
        task_sleep,
        max_running_tasks,
    )
    runtime = time.perf_counter() - start

    min_time = math.ceil(task_count / max_running_tasks) * task_sleep
    assert min_time <= runtime


def test_run_diamond(app_engine: AppEngine) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_diamond(app_engine, task_count, 0, task_sleep)
    runtime = time.perf_counter() - start

    layers = 3
    assert layers * task_sleep <= runtime


def test_run_reduce(app_engine: AppEngine) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_reduce(app_engine, task_count, 0, task_sleep)
    runtime = time.perf_counter() - start

    layers = 2
    assert layers * task_sleep <= runtime


def test_run_sequential(app_engine: AppEngine) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_sequential(app_engine, task_count, 0, task_sleep)
    runtime = time.perf_counter() - start

    assert task_count * task_sleep <= runtime
