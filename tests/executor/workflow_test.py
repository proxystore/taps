from __future__ import annotations

import uuid
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor

import pytest

from webs.executor.workflow import WorkflowExecutor
from webs.executor.workflow import WorkflowTask
from webs.executor.workflow import WorkflowTaskFuture


def test_task_future_forwards_result() -> None:
    base_future: Future[int] = Future()
    task_future = WorkflowTaskFuture(base_future, task_id=uuid.uuid4())

    called: list[bool] = []
    task_future.add_done_callback(lambda fut: called.append(True))

    assert not base_future.done()
    assert not task_future.done()

    assert task_future.set_running_or_notify_cancel()
    assert base_future.running()
    assert task_future.running()

    task_future.set_result(0)
    assert base_future.done()
    assert task_future.done()
    assert base_future.result() == 0
    assert task_future.result() == 0

    assert len(called) == 1


def test_task_future_forwards_cancel() -> None:
    base_future: Future[int] = Future()
    task_future = WorkflowTaskFuture(base_future, task_id=uuid.uuid4())

    task_future.cancel()
    assert base_future.cancelled()
    assert task_future.cancelled()


def test_task_future_forwards_exception() -> None:
    base_future: Future[int] = Future()
    task_future = WorkflowTaskFuture(base_future, task_id=uuid.uuid4())

    exception = ValueError('test')
    task_future.set_exception(exception)
    assert base_future.exception() == exception
    assert task_future.exception() == exception

    with pytest.raises(ValueError, match='test'):
        assert base_future.result()
    with pytest.raises(ValueError, match='test'):
        assert task_future.result()


def test_workflow_task_call() -> None:
    def sum_(values: list[int], *, start: int = 0):
        return sum(values, start=start)

    task = WorkflowTask(sum_)
    assert task([1, 2, 3], start=-6) == 0


def test_workflow_executor_submit(thread_executor: ThreadPoolExecutor) -> None:
    with WorkflowExecutor(thread_executor) as executor:
        future = executor.submit(sum, [1, 2, 3], start=-6)
        assert isinstance(future, WorkflowTaskFuture)
        assert future.result() == 0


def test_workflow_executor_map(thread_executor: ThreadPoolExecutor) -> None:
    with WorkflowExecutor(thread_executor) as executor:
        assert list(executor.map(abs, [1, -1])) == [1, 1]
