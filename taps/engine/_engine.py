from __future__ import annotations

import logging
import sys
import time
import uuid
from collections import namedtuple
from concurrent.futures import as_completed as as_completed_python
from concurrent.futures import Executor
from concurrent.futures import Future
from concurrent.futures import wait as wait_python
from traceback import TracebackException
from types import TracebackType
from typing import Any
from typing import Callable
from typing import cast
from typing import Generator
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import Sequence
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from dask.distributed import as_completed as as_completed_dask
from dask.distributed import Future as DaskFuture
from dask.distributed import wait as wait_dask

from taps.engine.task import ExceptionInfo
from taps.engine.task import Task
from taps.engine.task import task
from taps.engine.task import TaskInfo
from taps.engine.task import TaskResult
from taps.engine.transform import TaskTransformer
from taps.filter import Filter
from taps.future import FutureProtocol
from taps.logging import get_repr
from taps.logging import TRACE_LOG_LEVEL
from taps.record import NullRecordLogger
from taps.record import RecordLogger
from taps.transformer import Transformer

logger = logging.getLogger('taps.engine')

P = ParamSpec('P')
R = TypeVar('R')


def _result_or_cancel(
    future: TaskFuture[R],
    timeout: float | None = None,
) -> R:
    try:
        # Note: this used to be inside a try/finally block with the
        # finally arm calling future.cancel(). This was removed because
        # Dask Future have different cancel behavior that essentially
        # removes the data associated with the task. Wherea Python futures
        # are a no-op if you cancel a future that's already completed.
        return future.result(timeout)
    finally:
        # Break a reference cycle with the exception in self._exception
        del future


class TaskFuture(Generic[R]):
    """Task future.

    Note:
        This class should not be instantiated by clients.

    Args:
        future: Underlying future returned by the compute executor.
        info: Task information and metadata.
        transformer: Transformer used to resolve the task result.
    """

    def __init__(
        self,
        future: FutureProtocol[TaskResult[R]],
        info: TaskInfo,
        transformer: TaskTransformer[Any],
    ) -> None:
        self.info = info
        self.future = future
        self.transformer = transformer

    def cancel(self) -> bool:
        """Attempt to cancel the task.

        If the call is currently being executed or finished running and
        cannot be cancelled then the method will return `False`, otherwise
        the call will be cancelled and the method will return `True`.
        """
        return self.future.cancel()

    def done(self) -> bool:
        """Return `True` is the call was successfully cancelled or finished."""
        return self.future.done()

    def exception(self) -> BaseException | None:
        """Get the exception raised by the task or `None` if successful."""
        return self.future.exception()

    def result(self, timeout: float | None = None) -> R:
        """Get the result of the task.

        Args:
            timeout: If the task has not finished, wait up to `timeout`
                seconds.

        Returns:
            Task result if the task completed successfully.

        Raises:
            TimeoutError: If `timeout` is specified and the task does not
                complete within `timeout` seconds.
        """
        task_result = self.future.result(timeout=timeout)
        result = self.transformer.resolve(task_result.value)
        return result


class Engine:
    """Application execution engine.

    Args:
        executor: Task compute executor.
        filter_: Data filter.
        transformer: Data transformer.
        record_logger: Task record logger.
    """

    def __init__(
        self,
        executor: Executor,
        *,
        filter_: Filter | None = None,
        transformer: Transformer[Any] | None = None,
        record_logger: RecordLogger | None = None,
    ) -> None:
        self.executor = executor
        self.transformer: TaskTransformer[Any] = TaskTransformer(
            transformer,
            filter_,
        )
        self.record_logger = (
            record_logger if record_logger is not None else NullRecordLogger()
        )

        # Maps user provided functions to the Task object so they are only
        # wrapped once. This is only used for user provided functions that
        # were not already decorated with @task. This is tricky to type,
        # so we just use Any.
        self._registered_tasks: dict[Callable[[Any], Any], Task[Any, Any]] = {}

        # Internal bookkeeping
        self._running_tasks: dict[FutureProtocol[Any], TaskFuture[Any]] = {}
        self._total_tasks = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.shutdown()

    def __repr__(self) -> str:
        return (
            f'Engine(executor={get_repr(self.executor)}, '
            f'transformer={get_repr(self.transformer)}, '
            f'record_logger={get_repr(self.record_logger)}, '
            f'running_tasks={len(self._running_tasks)}, '
            f'tasks_executed={self.tasks_executed})'
        )

    @property
    def tasks_executed(self) -> int:
        """Total number of tasks submitted for execution."""
        return self._total_tasks

    def _task_done_callback(self, future: FutureProtocol[Any]) -> None:
        task_future = self._running_tasks.pop(future)
        try:
            execution_info = future.result().info
        except Exception as e:
            task_future.info.success = False
            tb = TracebackException.from_exception(e)
            info = ExceptionInfo(
                type=type(e).__name__,
                message=str(e),
                traceback=''.join(tb.format()),
            )
            task_future.info.exception = info
        else:
            task_future.info.success = True
            task_future.info.execution = execution_info
        task_future.info.received_time = time.time()
        self.record_logger.log(task_future.info.asdict())

    def _get_task(self, function: Callable[P, R]) -> Task[P, R]:
        if isinstance(function, Task):
            return function

        if function not in self._registered_tasks:
            function_as_task = task(function)
            logger.debug(
                f'Created task from function (name={function_as_task.name})',
            )
            self._registered_tasks[function] = function_as_task

        return cast(Task[P, R], self._registered_tasks[function])

    # Note: args/kwargs are typed as Any rather than P.args/P.kwargs
    # because the inputs may be TaskFuture types which will get translated
    # into the correct types before invoking the function.
    def submit(
        self,
        function: Callable[P, R],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> TaskFuture[R]:
        """Schedule the callable to be executed.

        This function can also accept
        [`TaskFuture`][taps.engine.TaskFuture] objects as input
        to denote dependencies between a parent and this child task.

        Args:
            function: [`Task`][taps.engine.task.Task] to execute or a function
                to turn into a [`Task`][taps.engine.task.Task].
            args: Positional arguments for the task.
            kwargs: Keyword arguments for the task.

        Returns:
            [`TaskFuture`][taps.engine.TaskFuture] object representing the \
            result of the execution of the callable accessible via \
            [`TaskFuture.result()`][taps.engine.TaskFuture.result].
        """
        task_id = uuid.uuid4()
        task = self._get_task(function)

        parents = [
            str(arg.info.task_id)
            for arg in (*args, *kwargs.values())
            if isinstance(arg, TaskFuture)
        ]
        info = TaskInfo(
            task_id=str(task_id),
            name=task.name,
            parent_task_ids=parents,
            submit_time=time.time(),
        )

        # Extract executor futures from inside TaskFuture objects
        args = tuple(
            arg.future if isinstance(arg, TaskFuture) else arg for arg in args
        )
        kwargs = {
            k: v.future if isinstance(v, TaskFuture) else v
            for k, v in kwargs.items()
        }

        args = self.transformer.transform_iterable(args)
        kwargs = self.transformer.transform_mapping(kwargs)

        future = self.executor.submit(
            task,
            *args,
            **kwargs,
            _transformer=self.transformer,
        )
        logger.log(
            TRACE_LOG_LEVEL,
            f'Submitted task to executor (id={task_id}, name={info.name}, '
            f'parents=[{", ".join(info.parent_task_ids)}])',
        )

        self._total_tasks += 1

        task_future = TaskFuture(future, info, self.transformer)
        self._running_tasks[future] = task_future
        future.add_done_callback(self._task_done_callback)

        return task_future

    def map(
        self,
        function: Callable[P, R],
        *iterables: Iterable[P.args],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[R]:
        """Map a function onto iterables of arguments.

        Args:
            function: A callable that will take as many arguments as there are
                passed iterables.
            iterables: Variable number of iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: Currently no supported. If greater than one, the
                iterables will be chopped into chunks of size chunksize
                and submitted to the executor. If set to one, the items in the
                list will be sent one at a time.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.
        """
        # Source: https://github.com/python/cpython/blob/ec1398e117fb142cc830495503dbdbb1ddafe941/Lib/concurrent/futures/_base.py#L583-L625
        if timeout is not None:
            end_time = timeout + time.monotonic()

        tasks = [self.submit(function, *args) for args in zip(*iterables)]

        # Yield must be hidden in closure so that the futures are submitted
        # before the first iterator value is required.
        def _result_iterator() -> Generator[R, None, None]:
            # reverse to keep finishing order
            tasks.reverse()
            while tasks:
                # Careful not to keep a reference to the popped future
                if timeout is None:
                    yield _result_or_cancel(tasks.pop())
                else:
                    yield _result_or_cancel(
                        tasks.pop(),
                        end_time - time.monotonic(),
                    )

        return _result_iterator()

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the executor.

        Args:
            wait: Wait on all pending futures to complete.
            cancel_futures: Cancel all pending futures that the executor
                has not started running. Only used in Python 3.9 and later.
        """
        if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
            self.executor.shutdown(
                wait=wait,
                cancel_futures=cancel_futures,
            )
        else:  # pragma: <3.9 cover
            self.executor.shutdown(wait=wait)
        self.transformer.close()
        self.record_logger.close()
        logger.debug('Engine shutdown')


def as_completed(
    tasks: Sequence[TaskFuture[R]],
    timeout: float | None = None,
) -> Generator[TaskFuture[R], None, None]:
    """Return an iterator which yields tasks as they complete.

    Args:
        tasks: Sequence of tasks.
        timeout: Seconds to wait for a task to complete. If no task completes
            in that time, a `TimeoutError` is raised. If timeout is `None`,
            there is no limit to the wait time.

    Returns:
        Iterator which yields futures as they complete (finished or cancelled \
        futures).
    """
    if len(tasks) == 0:
        return

    futures = {task.future: task for task in tasks}
    kwargs = {'timeout': timeout}

    # as_completed is tricky to type here.
    _as_completed: Any
    if len(tasks) == 0 or isinstance(tasks[0].future, Future):
        _as_completed = as_completed_python
    elif isinstance(tasks[0].future, DaskFuture):
        _as_completed = as_completed_dask
        if sys.version_info < (3, 9):  # pragma: <3.9 cover
            kwargs = {}
    else:  # pragma: no cover
        raise ValueError(f'Unsupported future type {type(tasks[0])}.')

    for completed in _as_completed(futures.keys(), **kwargs):
        yield futures[completed]


def wait(
    tasks: Sequence[TaskFuture[R]],
    timeout: float | None = None,
    return_when: str = 'ALL_COMPLETED',
) -> tuple[set[TaskFuture[R]], set[TaskFuture[R]]]:
    """Wait for tasks to finish.

    Args:
        tasks: Sequence of tasks to wait on.
        timeout: Maximum number of seconds to wait on tasks. Can be `None` to
            wait indefinitely.
        return_when: Either `"ALL_COMPLETED"` or `"FIRST_COMPLETED"`.

    Returns:
        Tuple containing the set of completed tasks and the set of not \
        completed tasks.
    """
    result = namedtuple('result', ['done', 'not_done'])

    if len(tasks) == 0:
        return result(set(), set())

    futures = {task.future: task for task in tasks}

    if len(tasks) == 0 or isinstance(tasks[0].future, Future):
        _wait = wait_python
    elif isinstance(tasks[0].future, DaskFuture):
        _wait = wait_dask
    else:  # pragma: no cover
        raise ValueError(f'Unsupported future type {type(tasks[0])}.')

    completed_futures, not_completed_futures = _wait(  # type: ignore[var-annotated]
        list(futures.keys()),  # type: ignore[arg-type]
        timeout=timeout,
        return_when=return_when,
    )

    completed_tasks = {futures[f] for f in completed_futures}
    not_completed_tasks = {futures[f] for f in not_completed_futures}

    return result(completed_tasks, not_completed_tasks)
