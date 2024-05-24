from __future__ import annotations

import math
import pathlib
import time
from unittest import mock

from webs.executor.workflow import WorkflowExecutor
from webs.wf.synthetic.config import SyntheticWorkflowConfig
from webs.wf.synthetic.config import WorkflowStructure
from webs.wf.synthetic.workflow import noop_task
from webs.wf.synthetic.workflow import run_bag_of_tasks
from webs.wf.synthetic.workflow import run_diamond
from webs.wf.synthetic.workflow import run_reduce
from webs.wf.synthetic.workflow import run_sequential
from webs.wf.synthetic.workflow import SyntheticWorkflow


def test_noop_task() -> None:
    output_size = 100
    sleep = 0.001

    start = time.perf_counter()
    result = noop_task(b'data', output_size=output_size, sleep=sleep)
    runtime = time.perf_counter() - start

    assert sleep <= runtime
    assert len(result) == output_size


def test_synthetic_workflow(
    workflow_executor: WorkflowExecutor,
    tmp_path: pathlib.Path,
) -> None:
    config = SyntheticWorkflowConfig(
        structure=WorkflowStructure.BAG,
        task_count=3,
        task_data_bytes=100,
        task_sleep=0.001,
        bag_max_running=3,
    )

    kinds = {
        WorkflowStructure.BAG: run_bag_of_tasks,
        WorkflowStructure.DIAMOND: run_diamond,
        WorkflowStructure.REDUCE: run_reduce,
        WorkflowStructure.SEQUENTIAL: run_sequential,
    }

    for kind, function in kinds.items():
        config.structure = kind
        workflow = SyntheticWorkflow.from_config(config)

        with mock.patch(
            f'webs.wf.synthetic.workflow.{function.__name__}',
        ) as mocked:
            workflow.run(workflow_executor, tmp_path)
            mocked.assert_called_once()


def test_run_bag_of_tasks(workflow_executor: WorkflowExecutor) -> None:
    task_count, task_sleep, max_running_tasks = 6, 0.001, 3

    start = time.perf_counter()
    run_bag_of_tasks(
        workflow_executor,
        task_count,
        0,
        task_sleep,
        max_running_tasks,
    )
    runtime = time.perf_counter() - start

    min_time = math.ceil(task_count / max_running_tasks) * task_sleep
    assert min_time <= runtime


def test_run_diamond(workflow_executor: WorkflowExecutor) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_diamond(workflow_executor, task_count, 0, task_sleep)
    runtime = time.perf_counter() - start

    layers = 3
    assert layers * task_sleep <= runtime


def test_run_reduce(workflow_executor: WorkflowExecutor) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_reduce(workflow_executor, task_count, 0, task_sleep)
    runtime = time.perf_counter() - start

    layers = 2
    assert layers * task_sleep <= runtime


def test_run_sequential(workflow_executor: WorkflowExecutor) -> None:
    task_count, task_sleep = 3, 0.001

    start = time.perf_counter()
    run_sequential(workflow_executor, task_count, 0, task_sleep)
    runtime = time.perf_counter() - start

    assert task_count * task_sleep <= runtime
