from __future__ import annotations

import logging
import os
import pathlib
import random
import sys
import time
import uuid

from taps.apps.configs.synthetic import WorkflowStructure
from taps.engine import as_completed
from taps.engine import Engine
from taps.engine import task
from taps.engine import TaskFuture
from taps.engine import wait
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)


class Data:
    """Synthetic task data."""

    def __init__(self, raw: bytes) -> None:
        self.raw = raw

    def __len__(self) -> int:
        return len(self.raw)

    def __sizeof__(self) -> int:
        return sys.getsizeof(self.raw)


def generate_data(size: int) -> Data:
    """Get random data of specified size.

    Uses `random.randbytes()` in Python 3.9 or newer and
    `os.urandom()` in Python 3.8 and older.

    Note:
        This class returns a `Data` object rather than a bytestring directly.
        This indirection is because some serializers skip [`bytes`][bytes]
        which will cause problems if ProxyStore is used in this application
        because the `Proxy[bytes]` will be an instance of [`bytes`][bytes] and
        won't get properly serialized. This is the case with Ray, for example.

    Args:
        size (int): size of byte string to return.

    Returns:
        random data.
    """
    max_bytes = int(1e9)
    if sys.version_info >= (3, 9) and size < max_bytes:  # pragma: >=3.9 cover
        raw = random.randbytes(size)
    else:  # pragma: <3.9 cover
        raw = os.urandom(size)
    return Data(raw)


def generate_length(mean: float, std_dev: float | None = None) -> float:
    """Get random task length with specified mean and standard deviation.

    Simple wrapper around random.gauss which generates a constant when
    std_dev is 0 or none, and guarantees the returned value is positive.

    Args:
        mean (float): mean length of sleep
        std_dev (float): standard deviation of task distribution

    Returns:
        random length.
    """
    if std_dev is None or std_dev == 0:
        return mean

    return max(0, random.gauss(mean, std_dev))


@task(name='noop')
def noop_task(
    *data: Data,
    output_size: int,
    sleep: float,
    task_id: uuid.UUID | None = None,
) -> Data:
    """No-op sleep task.

    Args:
        data: Input data.
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
    assert all(len(d.raw) >= 0 for d in data)
    result = generate_data(output_size)
    elapsed = (time.perf_counter_ns() - start) / 1e9

    # Remove elapsed time for generating result from remaining sleep time.
    time.sleep(max(0, sleep - elapsed))
    return result


@task(name='warmup')
def warmup_task() -> None:
    """No-op warmup task."""
    pass


def run_bag_of_tasks(
    engine: Engine,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
    max_running_tasks: int,
    task_std: float,
) -> None:
    """Run bag of tasks workflow."""
    max_running_tasks = min(max_running_tasks, task_count)
    start = time.monotonic()

    running_tasks = [
        engine.submit(
            noop_task,
            generate_data(task_data_bytes),
            output_size=task_data_bytes,
            sleep=generate_length(task_sleep, task_std),
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
        for finished_task in finished_tasks:
            exception = finished_task.exception()
            if isinstance(exception, Exception):  # pragma: no cover
                raise exception
            running_tasks.remove(finished_task)
            completed_tasks += 1

        new_task_count = min(len(finished_tasks), task_count - submitted_tasks)
        new_tasks = [
            engine.submit(
                noop_task,
                generate_data(task_data_bytes),
                output_size=task_data_bytes,
                sleep=generate_length(task_sleep, task_std),
                task_id=uuid.uuid4(),
            )
            for _ in range(new_task_count)
        ]
        running_tasks.extend(new_tasks)
        submitted_tasks += len(new_tasks)

        # Depending on how many tasks wait() returns, this may
        # not run. We could log *every* time wait() returns (i.e., every
        # loop), but this can result in a lot of log statements.
        if completed_tasks % max_running_tasks == 0:  # pragma: no cover
            rate = completed_tasks / (time.monotonic() - start)
            logger.log(
                APP_LOG_LEVEL,
                f'Completed {completed_tasks}/{task_count} tasks '
                f'(rate: {rate:.2f} tasks/s, running tasks: '
                f'{len(running_tasks)})',
            )

    wait(running_tasks, return_when='ALL_COMPLETED')
    # Validate task results are real
    assert all(len(task.result().raw) >= 0 for task in running_tasks)
    completed_tasks += len(running_tasks)
    rate = completed_tasks / (time.monotonic() - start)
    logger.log(
        APP_LOG_LEVEL,
        f'Completed {completed_tasks}/{task_count} (rate: {rate:.2f} tasks/s)',
    )


def run_diamond(
    engine: Engine,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
    task_std: float,
) -> None:
    """Run diamond workflow."""
    initial_task = engine.submit(
        noop_task,
        generate_data(task_data_bytes),
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
            sleep=generate_length(task_sleep, task_std),
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
    engine: Engine,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
    task_std: float,
) -> None:
    """Run reduce worklow."""
    map_tasks = [
        engine.submit(
            noop_task,
            generate_data(task_data_bytes),
            output_size=task_data_bytes,
            sleep=generate_length(task_sleep, task_std),
            task_id=uuid.uuid4(),
        )
        for _ in range(task_count)
    ]
    logger.log(APP_LOG_LEVEL, f'Submitted {task_count} initial tasks')

    reduce_task = engine.submit(
        noop_task,
        *map_tasks,
        output_size=task_data_bytes,
        sleep=generate_length(task_sleep, task_std),
        task_id=uuid.uuid4(),
    )
    logger.log(APP_LOG_LEVEL, 'Submitted reduce task')

    reduce_task.result()
    logger.log(APP_LOG_LEVEL, 'Reduce task completed')


def run_sequential(
    engine: Engine,
    task_count: int,
    task_data_bytes: int,
    task_sleep: float,
    task_std: float,
) -> None:
    """Run sequential workflow."""
    start = time.monotonic()
    initial_data = generate_data(task_data_bytes)
    tasks: list[TaskFuture[Data]] = []

    for i in range(task_count):
        input_data = initial_data if i == 0 else tasks[-1]
        task = engine.submit(
            noop_task,
            input_data,
            output_size=task_data_bytes,
            sleep=generate_length(task_sleep, task_std),
            task_id=uuid.uuid4(),
        )
        tasks.append(task)
        logger.log(
            APP_LOG_LEVEL,
            f'Submitted task {i + 1}/{task_count} '
            f'(task_id={task.info.task_id})',
        )

    for i, task in enumerate(as_completed(tasks)):
        logger.log(
            APP_LOG_LEVEL,
            f'Received task {i + 1}/{task_count} '
            f'(task_id: {task.info.task_id})',
        )

    # Validate the final result in the sequence
    assert len(tasks[-1].result().raw) >= 0

    rate = task_count / (time.monotonic() - start)
    logger.log(APP_LOG_LEVEL, f'Task completion rate: {rate:.3f} tasks/s')


class SyntheticApp:
    """Synthetic workflow application.

    Args:
        structure: Workflow structure.
        task_count: Number of tasks.
        task_data_bytes: Size of random input and output data of tasks.
        task_sleep: Seconds to sleep for in each task.
        bag_max_running: Maximum concurrently executing tasks in the "bag"
            workflow.
        warmup_tasks: Number of warmup tasks to submit before running the
            workflow.
        task_std: Deviation in task length to generate load imbalances
    """

    def __init__(
        self,
        structure: WorkflowStructure,
        task_count: int,
        task_data_bytes: int,
        task_sleep: float,
        bag_max_running: int | None,
        *,
        warmup_tasks: int = 0,
        task_std: float = 0,
    ) -> None:
        self.structure = structure
        self.task_count = task_count
        self.task_data_bytes = task_data_bytes
        self.task_sleep = task_sleep
        self.task_std = task_std
        self.bag_max_running = bag_max_running
        self.warmup_tasks = warmup_tasks

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        if self.warmup_tasks > 0:
            logger.log(
                APP_LOG_LEVEL,
                f'Submitting {self.warmup_tasks} warmup task(s)',
            )
            tasks = [
                engine.submit(warmup_task) for _ in range(self.warmup_tasks)
            ]
            for task in as_completed(tasks):
                task.result()
            logger.log(APP_LOG_LEVEL, 'Warmup task(s) completed')
        else:
            logger.log(APP_LOG_LEVEL, 'Skipping warmup tasks')

        logger.log(APP_LOG_LEVEL, f'Starting {self.structure.value} workflow')
        if self.structure == WorkflowStructure.BAG:
            assert self.bag_max_running is not None
            run_bag_of_tasks(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
                max_running_tasks=self.bag_max_running,
                task_std=self.task_std,
            )
        elif self.structure == WorkflowStructure.DIAMOND:
            run_diamond(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
                task_std=self.task_std,
            )
        elif self.structure == WorkflowStructure.REDUCE:
            run_reduce(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
                task_std=self.task_std,
            )
        elif self.structure == WorkflowStructure.SEQUENTIAL:
            run_sequential(
                engine,
                task_count=self.task_count,
                task_data_bytes=self.task_data_bytes,
                task_sleep=self.task_sleep,
                task_std=self.task_std,
            )
        else:
            raise AssertionError(
                f'Unhandled workflow structure type {self.structure}.',
            )
