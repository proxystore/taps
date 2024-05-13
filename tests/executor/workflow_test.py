from __future__ import annotations

import pathlib
import uuid
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor

from webs.data.file import PickleFileTransformer
from webs.data.transform import NullTransformer
from webs.data.transform import TaskDataTransformer
from webs.executor.workflow import _TaskWrapper
from webs.executor.workflow import WorkflowExecutor


def test_task_wrapper_call() -> None:
    def sum_(values: list[int], *, start: int = 0):
        return sum(values, start=start)

    task = _TaskWrapper(
        sum_,
        task_id=uuid.uuid4(),
        data_transformer=TaskDataTransformer(NullTransformer()),
    )
    assert task([1, 2, 3], start=-6) == 0


def test_workflow_executor_submit(workflow_executor: WorkflowExecutor) -> None:
    task = workflow_executor.submit(sum, [1, 2, 3], start=-6)
    assert isinstance(task.future, Future)
    assert task.future.result() == 0


def test_workflow_executor_map(workflow_executor: WorkflowExecutor) -> None:
    assert list(workflow_executor.map(abs, [1, -1])) == [1, 1]


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
        assert task.future.result() == 0
