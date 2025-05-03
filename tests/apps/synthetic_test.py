from __future__ import annotations

import math
import pathlib
import sys
import time
from unittest import mock

import pytest

from taps.apps.synthetic import Data
from taps.apps.synthetic import generate_data
from taps.apps.synthetic import noop_task
from taps.apps.synthetic import run_bag_of_tasks
from taps.apps.synthetic import run_diamond
from taps.apps.synthetic import run_reduce
from taps.apps.synthetic import run_sequential
from taps.apps.synthetic import SyntheticApp
from taps.apps.synthetic import WorkflowStructure
from taps.engine import Engine


@pytest.mark.parametrize('size', (0, 1, 10, 100))
def test_generate_data(size: int) -> None:
    b = generate_data(size)
    assert isinstance(b, Data)
    assert len(b) == size


def test_size_of_data() -> None:
    value = b'data'
    data = Data(value)
    # The Data object will have some additional garbage collection overhead.
    assert sys.getsizeof(data) >= sys.getsizeof(value)


def test_noop_task() -> None:
    output_size = 100
    sleep = 0.001

    start = time.perf_counter()
    result = noop_task(Data(b'data'), output_size=output_size, sleep=sleep)
    runtime = time.perf_counter() - start

    assert sleep <= runtime
    assert len(result) == output_size


def test_synthetic_app(
    engine: Engine,
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
            warmup_tasks=i,
        )

        with mock.patch(
            f'taps.apps.synthetic.{function.__name__}',
        ) as mocked:
            app.run(engine, tmp_path)
            mocked.assert_called_once()

        app.close()


def test_run_bag_of_tasks(engine: Engine) -> None:
    task_count, task_sleep, max_running_tasks, task_std = 6, 0.001, 3, 0.0001

    start = time.perf_counter()
    run_bag_of_tasks(
        engine,
        task_count,
        0,
        task_sleep,
        max_running_tasks,
        task_std,
    )
    runtime = time.perf_counter() - start

    min_time = math.ceil(task_count / max_running_tasks) * task_sleep
    assert min_time <= runtime


def test_run_diamond(engine: Engine) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_diamond(engine, task_count, 0, task_sleep, 0)
    runtime = time.perf_counter() - start

    layers = 3
    assert layers * task_sleep <= runtime


def test_run_reduce(engine: Engine) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_reduce(engine, task_count, 0, task_sleep, 0)
    runtime = time.perf_counter() - start

    layers = 2
    assert layers * task_sleep <= runtime


def test_run_sequential(engine: Engine) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_sequential(engine, task_count, 0, task_sleep, 0)
    runtime = time.perf_counter() - start

    assert task_count * task_sleep <= runtime
