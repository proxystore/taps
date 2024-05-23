from __future__ import annotations

import logging
import pathlib
import sys
import time

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import as_completed
from webs.executor.workflow import TaskFuture
from webs.executor.workflow import wait
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.synthetic.config import SyntheticWorkflowConfig
from webs.wf.synthetic.config import WorkflowStructure
from webs.wf.synthetic.utils import randbytes

logger = logging.getLogger(__name__)


def noop_task(*data: bytes, output_size: int, sleep: float) -> bytes:
    """No-op sleep task.

    Args:
        data: Input byte strings.
        output_size: Size in bytes of output byte-string.
        sleep: Minimum runtime of the task. Time required to generate the
            output data will be subtracted from this sleep time.

    Returns:
        Byte-string of length `output_size`.
    """
    assert all(isinstance(d, bytes) for d in data)
    start = time.perf_counter_ns()
    result = randbytes(output_size)
    elapsed = (time.perf_counter_ns() - start) / 1e9

    # Remove elapsed time for generating result from remaining sleep time.
    time.sleep(max(0, sleep - elapsed))
    return result


def run_bag_of_tasks(
    executor: WorkflowExecutor,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
    max_running_tasks: int,
) -> None:
    """Run bag of tasks workflow."""
    max_running_tasks = min(max_running_tasks, task_count)
    start = time.monotonic()

    running_tasks = [
        executor.submit(
            noop_task,
            randbytes(task_data_bytes),
            output_size=task_data_bytes,
            sleep=task_sleep,
        )
        for _ in range(max_running_tasks)
    ]
    logger.log(
        WORK_LOG_LEVEL,
        f'Submitted {max_running_tasks} initial tasks',
    )

    completed_tasks = 0
    submitted_tasks = len(running_tasks)

    while submitted_tasks < task_count:
        finished_tasks, _ = wait(running_tasks, return_when='FIRST_COMPLETED')
        for task in finished_tasks:
            assert task.exception() is None
            running_tasks.remove(task)
            completed_tasks += 1

        new_tasks = [
            executor.submit(
                noop_task,
                randbytes(task_data_bytes),
                output_size=task_data_bytes,
                sleep=task_sleep,
            )
            for _ in finished_tasks
        ]
        running_tasks.extend(new_tasks)
        submitted_tasks += len(new_tasks)

        if completed_tasks % max_running_tasks == 0:
            rate = completed_tasks / (time.monotonic() - start)
            logger.log(
                WORK_LOG_LEVEL,
                f'Completed {completed_tasks}/{task_count} tasks '
                f'(rate: {rate:.2f} tasks/s, running tasks: '
                f'{len(running_tasks)})',
            )

    wait(running_tasks, return_when='ALL_COMPLETED')
    completed_tasks += len(running_tasks)
    rate = completed_tasks / (time.monotonic() - start)
    logger.log(
        WORK_LOG_LEVEL,
        f'Completed {completed_tasks}/{task_count} (rate: {rate:.2f} tasks/s)',
    )


def run_diamond(
    executor: WorkflowExecutor,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
) -> None:
    """Run diamond workflow."""
    initial_task = executor.submit(
        noop_task,
        randbytes(task_data_bytes),
        output_size=task_data_bytes,
        sleep=task_sleep,
    )
    logger.log(WORK_LOG_LEVEL, 'Submitted initial task')

    intermediate_tasks = [
        executor.submit(
            noop_task,
            initial_task,
            output_size=task_data_bytes,
            sleep=task_sleep,
        )
        for _ in range(task_count)
    ]
    logger.log(
        WORK_LOG_LEVEL,
        f'Submitting {task_count} intermediate tasks',
    )

    final_task = executor.submit(
        noop_task,
        *intermediate_tasks,
        output_size=task_data_bytes,
        sleep=task_sleep,
    )
    logger.log(WORK_LOG_LEVEL, 'Submitted final task')

    final_task.result()
    logger.log(WORK_LOG_LEVEL, 'Final task completed')


def run_reduce(
    executor: WorkflowExecutor,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
) -> None:
    """Run reduce worklow."""
    map_tasks = [
        executor.submit(
            noop_task,
            randbytes(task_data_bytes),
            output_size=task_data_bytes,
            sleep=task_sleep,
        )
        for _ in range(task_count)
    ]
    logger.log(WORK_LOG_LEVEL, f'Submitted {task_count} initial tasks')

    reduce_task = executor.submit(
        noop_task,
        *map_tasks,
        output_size=task_data_bytes,
        sleep=task_sleep,
    )
    logger.log(WORK_LOG_LEVEL, 'Submitted reduce task')

    reduce_task.result()
    logger.log(WORK_LOG_LEVEL, 'Reduce task completed')


def run_sequential(
    executor: WorkflowExecutor,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
) -> None:
    """Run sequential workflow."""
    start = time.monotonic()
    initial_data = randbytes(task_data_bytes)
    tasks: list[TaskFuture[bytes]] = []

    for i in range(task_count):
        input_data = initial_data if i == 0 else tasks[-1]
        task = executor.submit(
            noop_task,
            input_data,
            output_size=task_data_bytes,
            sleep=task_sleep,
        )
        tasks.append(task)
        logger.log(
            WORK_LOG_LEVEL,
            f'Submitted task {i+1}/{task_count} '
            f'(task_id={task.info.task_id})',
        )

    for i, task in enumerate(as_completed(tasks)):
        assert task.done()
        logger.log(
            WORK_LOG_LEVEL,
            f'Received task {i+1}/{task_count} (task_id: {task.info.task_id})',
        )

    rate = task_count / (time.monotonic() - start)
    logger.log(WORK_LOG_LEVEL, f'Task completion rate: {rate:.3f} tasks/s')


class SyntheticWorkflow(ContextManagerAddIn):
    """Synthetic workflow.

    Args:
        structure: Workflow structure.
        task_count: Number of tasks.
        task_data_bytes: Size of random input and output data of tasks.
        task_sleep: Seconds to sleep for in each task.
        bag_max_running: Maximum concurrently executing tasks in the "bag"
            workflow.
    """

    name = 'synthetic'
    config_type = SyntheticWorkflowConfig

    def __init__(  # noqa: PLR0913
        self,
        structure: WorkflowStructure,
        task_count: int,
        task_data_bytes: int,
        task_sleep: float,
        bag_max_running: int,
    ) -> None:
        self.structure = structure
        self.task_count = task_count
        self.task_data_bytes = task_data_bytes
        self.task_sleep = task_sleep
        self.bag_max_running = bag_max_running
        super().__init__()

    @classmethod
    def from_config(cls, config: SyntheticWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(
            structure=config.structure,
            task_count=config.task_count,
            task_data_bytes=config.task_data_bytes,
            task_sleep=config.task_sleep,
            bag_max_running=config.bag_max_running,
        )

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        logger.log(WORK_LOG_LEVEL, f'Starting {self.structure.value} workflow')
        if self.structure == WorkflowStructure.BAG:
            run_bag_of_tasks(
                executor,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
                max_running_tasks=self.bag_max_running,
            )
        elif self.structure == WorkflowStructure.DIAMOND:
            run_diamond(
                executor,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
            )
        elif self.structure == WorkflowStructure.REDUCE:
            run_reduce(
                executor,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
            )
        elif self.structure == WorkflowStructure.SEQUENTIAL:
            run_sequential(
                executor,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
            )
        else:
            raise AssertionError(
                f'Unhandled workflow structure type {self.structure}.',
            )
