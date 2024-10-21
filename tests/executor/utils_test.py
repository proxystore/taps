from __future__ import annotations

import re
from concurrent.futures import CancelledError
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Generator

import pytest

from taps.executor.utils import _Task
from taps.executor.utils import FutureDependencyExecutor


@pytest.fixture
def executor(
    thread_executor: ThreadPoolExecutor,
) -> Generator[FutureDependencyExecutor, None, None]:
    with FutureDependencyExecutor(thread_executor) as executor:
        yield executor


def abs_(value: Future[int] | int) -> int:
    value = value if isinstance(value, int) else value.result()
    return abs(value)


def test_dag_executor_submit(executor: FutureDependencyExecutor) -> None:
    future = executor.submit(sum, [1, 2, 3], start=-6)
    assert future.result() == 0


def test_dag_executor_map(executor: FutureDependencyExecutor) -> None:
    values = [1, 0, -1]
    results = executor.map(abs_, values)
    assert list(results) == list(map(abs, values))


def test_dag_executor_shutdown(thread_executor: ThreadPoolExecutor) -> None:
    executor = FutureDependencyExecutor(thread_executor)
    executor.shutdown()
    with pytest.raises(
        RuntimeError,
        match='cannot schedule new futures after shutdown',
    ):
        executor.submit(sum, [1, 2, 3])


def test_dag_executor_map_value_error(
    executor: FutureDependencyExecutor,
) -> None:
    with pytest.raises(ValueError, match='chunksize must be >= 1.'):
        executor.map(abs, [], chunksize=0)


def test_dag_executor_chained_dependencies_threads(
    thread_executor: ThreadPoolExecutor,
) -> None:
    with FutureDependencyExecutor(thread_executor) as executor:
        fut1 = executor.submit(sum, [-1, -2, -3])
        fut2 = executor.submit(abs_, fut1)
        expected = 6
        assert fut2.result() == expected


def test_dag_executor_chained_dependencies_process(
    process_executor: ProcessPoolExecutor,
) -> None:
    with FutureDependencyExecutor(process_executor) as executor:
        fut1 = executor.submit(sum, [-1, -2, -3])
        fut2 = executor.submit(abs_, fut1)
        expected = 6
        assert fut2.result() == expected


def test_task_no_futures(thread_executor: ThreadPoolExecutor) -> None:
    client_future: Future[int] = Future()
    task = _Task(thread_executor, sum, ([1, -1],), {}, client_future)
    assert client_future.result() == 0
    assert task.task_future is not None
    assert task.task_future.done()


def test_task_future_basic_function(
    thread_executor: ThreadPoolExecutor,
) -> None:
    client_future: Future[int] = Future()
    arg_future: Future[list[int]] = Future()
    kwarg_future: Future[int] = Future()

    task = _Task(
        thread_executor,
        sum,
        (arg_future,),
        {'start': kwarg_future},
        client_future,
    )

    assert not client_future.running()
    arg_future.set_result([1, -1])
    assert not client_future.running()
    kwarg_future.set_result(0)
    assert client_future.running() or client_future.done()

    assert client_future.result() == 0
    assert task.task_future is not None
    assert task.task_future.done()


def test_task_client_future_cancelled(
    thread_executor: ThreadPoolExecutor,
) -> None:
    client_future: Future[int] = Future()
    assert client_future.cancel()
    task = _Task(thread_executor, sum, ([1, -1],), {}, client_future)
    with pytest.raises(CancelledError):
        client_future.result()
    assert task.task_future is None


def test_task_client_future_exception(
    thread_executor: ThreadPoolExecutor,
) -> None:
    client_future: Future[int] = Future()
    task = _Task(thread_executor, sum, ('nan',), {}, client_future)
    with pytest.raises(
        TypeError,
        match=re.escape('unsupported operand type(s) for'),
    ):
        client_future.result()
    assert task.task_future is not None
    assert task.task_future.done()


def test_task_arg_future_cancelled(
    thread_executor: ThreadPoolExecutor,
) -> None:
    client_future: Future[int] = Future()
    arg_future: Future[list[int]] = Future()
    task = _Task(thread_executor, sum, (arg_future,), {}, client_future)
    assert arg_future.cancel()
    with pytest.raises(CancelledError):
        client_future.result()
    assert task.task_future is None


def test_task_arg_future_exception(
    thread_executor: ThreadPoolExecutor,
) -> None:
    client_future: Future[int] = Future()
    arg_future: Future[list[int]] = Future()
    task = _Task(thread_executor, sum, (arg_future,), {}, client_future)
    arg_future.set_exception(ValueError('test'))
    with pytest.raises(ValueError, match='test'):
        client_future.result()
    assert task.task_future is None


def test_task_future_cancelled(
    thread_executor: ThreadPoolExecutor,
) -> None:
    client_future: Future[int] = Future()
    arg_future: Future[list[int]] = Future()
    task_future: Future[int] = Future()

    task = _Task(thread_executor, sum, (arg_future,), {}, client_future)
    task.task_future = task_future
    task.task_future.add_done_callback(task._task_future_callback)
    assert task.task_future.cancel()
    with pytest.raises(CancelledError):
        client_future.result()
