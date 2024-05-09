from __future__ import annotations

import pathlib
import time

from webs.executor.workflow import WorkflowExecutor
from webs.wf.synthetic.config import SyntheticWorkflowConfig
from webs.wf.synthetic.workflow import noop_task
from webs.wf.synthetic.workflow import SyntheticWorkflow


def test_noop_task() -> None:
    output_size = 100
    sleep = 0.001

    start = time.perf_counter()
    result = noop_task(b'data', output_size, sleep)
    runtime = time.perf_counter() - start

    assert runtime >= sleep
    assert len(result) == output_size


def test_run_synthetic_workflow(
    workflow_executor: WorkflowExecutor,
    tmp_path: pathlib.Path,
) -> None:
    config = SyntheticWorkflowConfig(
        task_count=3,
        task_data_bytes=100,
        task_sleep=0.001,
    )
    workflow = SyntheticWorkflow.from_config(config)

    start = time.perf_counter()
    workflow.run(workflow_executor, tmp_path)
    runtime = time.perf_counter() - start

    assert runtime >= config.task_count * config.task_sleep
