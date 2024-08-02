from __future__ import annotations

import pathlib
import time
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor

from taps.engine import as_completed
from taps.engine import Engine
from taps.engine import TaskFuture
from taps.engine import wait
from taps.engine.task import TaskInfo
from taps.engine.task import TaskResult
from taps.engine.transform import TaskTransformer
from taps.executor import DaskDistributedExecutor
from taps.executor import FutureDependencyExecutor
from taps.filter import NullFilter
from taps.transformer import NullTransformer
from taps.transformer import PickleFileTransformer
from testing.record import SimpleRecordLogger


def test_task_future_exception() -> None:
    future: Future[TaskResult[int]] = Future()

    task = TaskFuture(
        future,
        TaskInfo('test', 'test', [], 0),
        TaskTransformer(NullTransformer(), NullFilter()),
    )

    exception = RuntimeError()
    future.set_exception(exception)
    assert task.exception() == exception


def test_engine_submit(app_engine: Engine) -> None:
    task = app_engine.submit(sum, [1, 2, 3], start=-6)
    assert isinstance(task, TaskFuture)
    assert task.result() == 0
    assert not task.cancel()
    assert app_engine.tasks_executed == 1


def test_engine_map(app_engine: Engine) -> None:
    x = [1, -1]
    assert list(app_engine.map(abs, x)) == [abs(v) for v in x]
    assert app_engine.tasks_executed == len(x)


def test_engine_dask(
    dask_executor: DaskDistributedExecutor,
) -> None:
    with Engine(dask_executor) as executor:
        task = executor.submit(sum, [1, 2, 3], start=-6)
        assert task.result() == 0

        assert list(executor.map(abs, [1, -1])) == [1, 1]


def test_engine_map_timeout(
    app_engine: Engine,
) -> None:
    assert list(app_engine.map(abs, [1, -1], timeout=1)) == [1, 1]


def test_engine_data_transformer(
    thread_executor: ThreadPoolExecutor,
    tmp_path: pathlib.Path,
) -> None:
    with Engine(
        thread_executor,
        data_transformer=PickleFileTransformer(tmp_path),
    ) as executor:
        task = executor.submit(sum, [1, 2, 3], start=-6)
        assert task.result() == 0


def test_engine_record_logging(
    thread_executor: ThreadPoolExecutor,
    tmp_path: pathlib.Path,
) -> None:
    with SimpleRecordLogger() as logger:
        with Engine(
            FutureDependencyExecutor(thread_executor),
            record_logger=logger,
        ) as executor:
            task1 = executor.submit(sum, [1, 2, 3], start=-6)
            task2 = executor.submit(sum, [1], start=task1)
            map_result = list(executor.map(sum, ([1, -1], [0, 1])))

            assert task1.result() == 0
            assert task2.result() == 1
            assert map_result == [0, 1]

        # Four tasks: task1, task2, and the two tasks in the map
        assert len(logger.records) == 4  # noqa: PLR2004

        for record in logger.records:
            if record['task_id'] == str(task2.info.task_id):
                assert str(task1.info.task_id) in record['parent_task_ids']
                break
        else:  # pragma: no cover
            raise RuntimeError(
                f'Did not find record for task {task1.info.task_id}',
            )


def test_engine_record_logging_exception(
    thread_executor: ThreadPoolExecutor,
    tmp_path: pathlib.Path,
) -> None:
    def _error() -> None:
        raise ValueError('bad task')

    with SimpleRecordLogger() as logger:
        with Engine(
            FutureDependencyExecutor(thread_executor),
            record_logger=logger,
        ) as executor:
            task = executor.submit(_error)

            assert task.exception() is not None

        assert len(logger.records) == 1
        task_info = logger.records[0]

        assert not task_info['success']
        assert task_info['exception']['type'] == 'ValueError'
        assert task_info['exception']['message'] == 'bad task'
        assert len(task_info['exception']['traceback']) > 0


def test_as_completed(app_engine: Engine) -> None:
    tasks = [app_engine.submit(sum, [x, 1]) for x in range(5)]
    completed = as_completed(tasks)
    assert set(tasks) == set(completed)


def test_as_completed_dask(dask_executor: DaskDistributedExecutor) -> None:
    with Engine(dask_executor) as executor:
        tasks = [executor.submit(sum, [x, 1]) for x in range(5)]
        completed_results = {task.result() for task in as_completed(tasks)}
        assert completed_results == set(range(1, 6))


def test_as_completed_empty() -> None:
    assert len(list(as_completed([]))) == 0


def test_wait() -> None:
    fast_future: Future[TaskResult[int]] = Future()
    fast_future.set_result(TaskResult(0, None))  # type: ignore[arg-type]
    slow_future: Future[TaskResult[int]] = Future()

    fast_task = TaskFuture(
        fast_future,
        TaskInfo('fast-id', 'fast', [], 0),
        TaskTransformer(NullTransformer(), NullFilter()),
    )
    slow_task = TaskFuture(
        slow_future,
        TaskInfo('slow-id', 'slow', [], 0),
        TaskTransformer(NullTransformer(), NullFilter()),
    )

    timeout = 0.001
    start = time.perf_counter()
    completed, not_completed = wait([fast_task, slow_task], timeout=timeout)
    wait_time = time.perf_counter() - start

    assert wait_time >= timeout
    assert completed == {fast_task}
    assert not_completed == {slow_task}


def test_wait_dask(dask_executor: DaskDistributedExecutor) -> None:
    with Engine(dask_executor) as executor:
        tasks = [executor.submit(sum, [x, 1]) for x in range(5)]
        completed, not_completed = wait(tasks)
        assert len(completed) == len(tasks)
        assert len(not_completed) == 0
        results = {task.result() for task in completed}
        assert results == set(range(1, 6))


def test_wait_empty() -> None:
    done, not_done = wait([])  # type: ignore[var-annotated]
    assert len(done) == 0
    assert len(not_done) == 0
