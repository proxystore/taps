from __future__ import annotations

import logging
import pathlib
import sys
import time
from concurrent.futures import as_completed
from concurrent.futures import Future

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.data import randbytes
from webs.executor.workflow import WorkflowExecutor
from webs.executor.workflow import WorkflowTask
from webs.logging import WORK_LOG_LEVEL
from webs.wf.synthetic.config import SyntheticWorkflowConfig
from webs.workflow import register

logger = logging.getLogger(__name__)


def noop_task(data: bytes, output_size: int, sleep: float) -> bytes:
    """No-op sleep task.

    Args:
        data: Input byte string.
        output_size: Size in bytes of output byte-string.
        sleep: Minimum runtime of the task. Time required to generate the
            output data will be subtracted from this sleep time.

    Returns:
        Byte-string of length `output_size`.
    """
    start = time.perf_counter_ns()
    result = randbytes(output_size)
    elapsed = (time.perf_counter_ns() - start) / 1e9

    # Remove elapsed time for generating result from remaining
    # sleep time.
    time.sleep(max(0, sleep - elapsed))
    return result


@register()
class SyntheticWorkflow(ContextManagerAddIn):
    """Synthetic workflow.

    Args:
        config: Workflow configuration.
    """

    name = 'synthetic'
    config_type = SyntheticWorkflowConfig

    def __init__(self, config: SyntheticWorkflowConfig) -> None:
        self.config = config
        super().__init__()

    @classmethod
    def from_config(cls, config: SyntheticWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(config)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        input_data: bytes | Future[bytes] = randbytes(
            self.config.task_data_bytes,
        )
        tasks: dict[Future[bytes], WorkflowTask[bytes]] = {}

        for i in range(self.config.task_count):
            task = executor.submit(
                noop_task,
                input_data,
                output_size=self.config.task_data_bytes,
                sleep=self.config.task_sleep,
            )
            input_data = task.future
            tasks[task.future] = task
            logger.log(
                WORK_LOG_LEVEL,
                f'Submitted task {i+1}/{self.config.task_count} '
                f'(task_id={task.task_id})',
            )

        for i, future in enumerate(as_completed(tasks.keys())):
            task = tasks[future]
            logger.log(
                WORK_LOG_LEVEL,
                f'Received task {i+1}/{self.config.task_count} '
                f'(task_id={task.task_id})',
            )
