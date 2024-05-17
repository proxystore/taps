from __future__ import annotations

import pathlib
import time
import uuid
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor

from testing.record import SimpleRecordLogger
from webs.data.file import PickleFileTransformer
from webs.data.transform import NullTransformer
from webs.data.transform import TaskDataTransformer
from webs.executor.dag import DAGExecutor
from webs.executor.dask import DaskDistributedExecutor
from webs.executor.workflow import _TaskResult
from webs.executor.workflow import _TaskWrapper
from webs.executor.workflow import as_completed
from webs.executor.workflow import TaskFuture
from webs.executor.workflow import TaskInfo
from webs.executor.workflow import wait
from webs.executor.workflow import WorkflowExecutor


def test_task_wrapper_call() -> None:
    def sum_(values: list[int], *, start: int = 0):
        return sum(values, start=start)

    task = _TaskWrapper(
        sum_,
        task_id=uuid.uuid4(),
        data_transformer=TaskDataTransformer(NullTransformer()),
    )
    assert task([1, 2, 3], start=-6).result == 0


def test_workflow_executor_submit(workflow_executor: WorkflowExecutor) -> None:
    task = workflow_executor.submit(sum, [1, 2, 3], start=-6)
    assert isinstance(task, TaskFuture)
    assert task.result() == 0
    assert not task.cancel()
    assert workflow_executor.tasks_executed == 1


def test_workflow_executor_map(workflow_executor: WorkflowExecutor) -> None:
    x = [1, -1]
    assert list(workflow_executor.map(abs, x)) == [abs(v) for v in x]
    assert workflow_executor.tasks_executed == len(x)


def test_workflow_executor_dask(
    dask_executor: DaskDistributedExecutor,
) -> None:
    with WorkflowExecutor(dask_executor) as executor:
        task = executor.submit(sum, [1, 2, 3], start=-6)
        assert task.result() == 0

        assert list(executor.map(abs, [1, -1])) == [1, 1]


def test_workflow_executor_map_timeout(
    workflow_executor: WorkflowExecutor,
) -> None:
    assert list(workflow_executor.map(abs, [1, -1], timeout=1)) == [1, 1]


def test_workflow_executor_data_transformer(
    thread_executor: ThreadPoolExecutor,
    tmp_path: pathlib.Path,
) -> None:
    transformer = TaskDataTransformer(PickleFileTransformer(tmp_path))
    with WorkflowExecutor(
        thread_executor,
        data_transformer=transformer,
    ) as executor:
        task = executor.submit(sum, [1, 2, 3], start=-6)
        assert task.result() == 0


def test_workflow_executor_record_logging(
    thread_executor: ThreadPoolExecutor,
    tmp_path: pathlib.Path,
) -> None:
    with SimpleRecordLogger() as logger:
        with WorkflowExecutor(
            DAGExecutor(thread_executor),
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
        assert str(task1.info.task_id) in logger.records[1]['parent_task_ids']


def test_as_completed(workflow_executor: WorkflowExecutor) -> None:
    tasks = [workflow_executor.submit(sum, [x, 1]) for x in range(5)]
    completed = as_completed(tasks)
    assert set(tasks) == set(completed)


def test_as_completed_dask(dask_executor: DaskDistributedExecutor) -> None:
    with WorkflowExecutor(dask_executor) as executor:
        tasks = [executor.submit(sum, [x, 1]) for x in range(5)]
        completed_results = {task.result() for task in as_completed(tasks)}
        assert completed_results == set(range(1, 6))


def test_wait() -> None:
    fast_future: Future[_TaskResult[int]] = Future()
    fast_future.set_result(_TaskResult(0, None))  # type: ignore[arg-type]
    slow_future: Future[_TaskResult[int]] = Future()

    fast_task = TaskFuture(
        fast_future,
        TaskInfo('fast-id', 'fast', [], 0),
        TaskDataTransformer(NullTransformer()),
    )
    slow_task = TaskFuture(
        slow_future,
        TaskInfo('slow-id', 'slow', [], 0),
        TaskDataTransformer(NullTransformer()),
    )

    timeout = 0.001
    start = time.perf_counter()
    completed, not_completed = wait([fast_task, slow_task], timeout=timeout)
    wait_time = time.perf_counter() - start

    assert wait_time >= timeout
    assert completed == {fast_task}
    assert not_completed == {slow_task}


def test_wait_dask(dask_executor: DaskDistributedExecutor) -> None:
    with WorkflowExecutor(dask_executor) as executor:
        tasks = [executor.submit(sum, [x, 1]) for x in range(5)]
        completed, not_completed = wait(tasks)
        assert len(completed) == len(tasks)
        assert len(not_completed) == 0
        results = {task.result() for task in completed}
        assert results == set(range(1, 6))
