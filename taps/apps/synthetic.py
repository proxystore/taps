from __future__ import annotations

import enum
import logging
import os
import pathlib
import random
import sys
import time
import uuid

from taps.engine import AppEngine
from taps.engine import as_completed
from taps.engine import TaskFuture
from taps.engine import wait
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)


def randbytes(size: int) -> bytes:
    """Get random byte string of specified size.

    Uses `random.randbytes()` in Python 3.9 or newer and
    `os.urandom()` in Python 3.8 and older.

    Args:
        size (int): size of byte string to return.

    Returns:
        random byte string.
    """
    max_bytes = int(1e9)
    if sys.version_info >= (3, 9) and size < max_bytes:  # pragma: >=3.9 cover
        return random.randbytes(size)
    else:  # pragma: <3.9 cover
        return os.urandom(size)


def noop_task(
    *data: bytes,
    output_size: int,
    sleep: float,
    task_id: uuid.UUID | None = None,
) -> bytes:
    """No-op sleep task.

    Args:
        data: Input byte strings.
        output_size: Size in bytes of output byte-string.
        sleep: Minimum runtime of the task. Time required to generate the
            output data will be subtracted from this sleep time.
        task_id: Optional unique task ID to prevent engines from caching
            the task result.

    Returns:
        Byte-string of length `output_size`.
    """
    start = time.perf_counter_ns()
    # Validate the data is real
    assert all(len(d) >= 0 for d in data)
    result = randbytes(output_size)
    elapsed = (time.perf_counter_ns() - start) / 1e9

    # Remove elapsed time for generating result from remaining sleep time.
    time.sleep(max(0, sleep - elapsed))
    return result


def warmup_task() -> None:
    """No-op warmup task."""
    pass


def run_bag_of_tasks(
    engine: AppEngine,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
    max_running_tasks: int,
) -> None:
    """Run bag of tasks workflow."""
    max_running_tasks = min(max_running_tasks, task_count)
    start = time.monotonic()

    running_tasks = [
        engine.submit(
            noop_task,
            randbytes(task_data_bytes),
            output_size=task_data_bytes,
            sleep=task_sleep,
            task_id=uuid.uuid4(),
        )
        for _ in range(max_running_tasks)
    ]
    logger.log(
        APP_LOG_LEVEL,
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
            engine.submit(
                noop_task,
                randbytes(task_data_bytes),
                output_size=task_data_bytes,
                sleep=task_sleep,
                task_id=uuid.uuid4(),
            )
            for _ in finished_tasks
        ]
        running_tasks.extend(new_tasks)
        submitted_tasks += len(new_tasks)

        if completed_tasks % max_running_tasks == 0:
            rate = completed_tasks / (time.monotonic() - start)
            logger.log(
                APP_LOG_LEVEL,
                f'Completed {completed_tasks}/{task_count} tasks '
                f'(rate: {rate:.2f} tasks/s, running tasks: '
                f'{len(running_tasks)})',
            )

    wait(running_tasks, return_when='ALL_COMPLETED')
    # Validate task results are real
    assert all(len(task.result()) >= 0 for task in running_tasks)
    completed_tasks += len(running_tasks)
    rate = completed_tasks / (time.monotonic() - start)
    logger.log(
        APP_LOG_LEVEL,
        f'Completed {completed_tasks}/{task_count} (rate: {rate:.2f} tasks/s)',
    )


def run_diamond(
    engine: AppEngine,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
) -> None:
    """Run diamond workflow."""
    initial_task = engine.submit(
        noop_task,
        randbytes(task_data_bytes),
        output_size=task_data_bytes,
        sleep=task_sleep,
        task_id=uuid.uuid4(),
    )
    logger.log(APP_LOG_LEVEL, 'Submitted initial task')

    intermediate_tasks = [
        engine.submit(
            noop_task,
            initial_task,
            output_size=task_data_bytes,
            sleep=task_sleep,
            task_id=uuid.uuid4(),
        )
        for _ in range(task_count)
    ]
    logger.log(
        APP_LOG_LEVEL,
        f'Submitting {task_count} intermediate tasks',
    )

    final_task = engine.submit(
        noop_task,
        *intermediate_tasks,
        output_size=task_data_bytes,
        sleep=task_sleep,
        task_id=uuid.uuid4(),
    )
    logger.log(APP_LOG_LEVEL, 'Submitted final task')

    final_task.result()
    logger.log(APP_LOG_LEVEL, 'Final task completed')


def run_reduce(
    engine: AppEngine,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
) -> None:
    """Run reduce worklow."""
    map_tasks = [
        engine.submit(
            noop_task,
            randbytes(task_data_bytes),
            output_size=task_data_bytes,
            sleep=task_sleep,
            task_id=uuid.uuid4(),
        )
        for _ in range(task_count)
    ]
    logger.log(APP_LOG_LEVEL, f'Submitted {task_count} initial tasks')

    reduce_task = engine.submit(
        noop_task,
        *map_tasks,
        output_size=task_data_bytes,
        sleep=task_sleep,
        task_id=uuid.uuid4(),
    )
    logger.log(APP_LOG_LEVEL, 'Submitted reduce task')

    reduce_task.result()
    logger.log(APP_LOG_LEVEL, 'Reduce task completed')


def run_sequential(
    engine: AppEngine,
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
        task = engine.submit(
            noop_task,
            input_data,
            output_size=task_data_bytes,
            sleep=task_sleep,
            task_id=uuid.uuid4(),
        )
        tasks.append(task)
        logger.log(
            APP_LOG_LEVEL,
            f'Submitted task {i+1}/{task_count} '
            f'(task_id={task.info.task_id})',
        )

    for i, task in enumerate(as_completed(tasks)):
        assert task.done()
        logger.log(
            APP_LOG_LEVEL,
            f'Received task {i+1}/{task_count} (task_id: {task.info.task_id})',
        )

    # Validate the final result in the sequence
    assert len(tasks[-1].result()) >= 0

    rate = task_count / (time.monotonic() - start)
    logger.log(APP_LOG_LEVEL, f'Task completion rate: {rate:.3f} tasks/s')


class WorkflowStructure(enum.Enum):
    """Workflow structure types."""

    BAG = 'bag'
    DIAMOND = 'diamond'
    REDUCE = 'reduce'
    SEQUENTIAL = 'sequential'


class SyntheticApp:
    """Synthetic workflow application.

    Args:
        structure: Workflow structure.
        task_count: Number of tasks.
        task_data_bytes: Size of random input and output data of tasks.
        task_sleep: Seconds to sleep for in each task.
        bag_max_running: Maximum concurrently executing tasks in the "bag"
            workflow.
    """

    def __init__(
        self,
        structure: WorkflowStructure,
        task_count: int,
        task_data_bytes: int,
        task_sleep: float,
        bag_max_running: int,
        *,
        warmup_task: bool = True,
    ) -> None:
        self.structure = structure
        self.task_count = task_count
        self.task_data_bytes = task_data_bytes
        self.task_sleep = task_sleep
        self.bag_max_running = bag_max_running
        self.warmup_task = warmup_task

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: AppEngine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        if self.warmup_task:
            logger.log(APP_LOG_LEVEL, 'Submitting warmup task')
            engine.submit(warmup_task).result()
            logger.log(APP_LOG_LEVEL, 'Warmup task completed')

        logger.log(APP_LOG_LEVEL, f'Starting {self.structure.value} workflow')
        if self.structure == WorkflowStructure.BAG:
            run_bag_of_tasks(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
                max_running_tasks=self.bag_max_running,
            )
        elif self.structure == WorkflowStructure.DIAMOND:
            run_diamond(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
            )
        elif self.structure == WorkflowStructure.REDUCE:
            run_reduce(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
            )
        elif self.structure == WorkflowStructure.SEQUENTIAL:
            run_sequential(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
            )
        else:
            raise AssertionError(
                f'Unhandled workflow structure type {self.structure}.',
            )
