from __future__ import annotations

import dataclasses
import functools
import socket
import sys
import time
import uuid
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

from taps.engine.transform import TaskTransformer
from taps.filter import Filter
from taps.filter import NullFilter
from taps.record import NullRecordLogger
from taps.record import RecordLogger
from taps.transformer.null import NullTransformer
from taps.transformer.protocol import Transformer

P = ParamSpec('P')
T = TypeVar('T')


@dataclasses.dataclass
class ExceptionInfo:
    """Task exception information."""

    type: str
    message: str
    traceback: str


@dataclasses.dataclass
class ExecutionInfo:
    """Task execution information."""

    hostname: str
    execution_start_time: float
    execution_end_time: float
    task_start_time: float
    task_end_time: float
    input_transform_start_time: float
    input_transform_end_time: float
    result_transform_start_time: float
    result_transform_end_time: float


@dataclasses.dataclass
class TaskInfo:
    """Task information."""

    task_id: str
    function_name: str
    parent_task_ids: list[str]
    submit_time: float
    received_time: float | None = None
    success: bool | None = None
    exception: ExceptionInfo | None = None
    execution: ExecutionInfo | None = None


@dataclasses.dataclass
class _TaskResult(Generic[T]):
    result: T
    info: ExecutionInfo


class _TaskWrapper(Generic[P, T]):
    """Task wrapper.

    Args:
        function: Function that represents the work associated with the task.
        task_id: Unique UUID of the task.
    """

    def __init__(
        self,
        function: Callable[P, T],
        *,
        task_id: uuid.UUID,
        data_transformer: TaskTransformer[Any],
    ) -> None:
        self.function = function
        self.task_id = uuid.uuid4() if task_id is None else task_id
        self.data_transformer = data_transformer
        #  Make this class instance "look" like `function`.
        functools.update_wrapper(self, function)

    def __call__(self, *args: Any, **kwargs: Any) -> _TaskResult[T]:
        """Call the function associated with the task."""
        execution_start_time = time.time()
        args = tuple(
            arg.result if isinstance(arg, _TaskResult) else arg for arg in args
        )
        kwargs = {
            k: v.result if isinstance(v, _TaskResult) else v
            for k, v in kwargs.items()
        }

        input_transform_start_time = time.time()
        args = self.data_transformer.resolve_iterable(args)
        kwargs = self.data_transformer.resolve_mapping(kwargs)
        input_transform_end_time = time.time()

        task_start_time = time.time()
        result = self.function(*args, **kwargs)
        task_end_time = time.time()

        result_transform_start_time = time.time()
        result = self.data_transformer.transform(result)
        result_transform_end_time = time.time()

        execution_end_time = time.time()

        info = ExecutionInfo(
            hostname=socket.gethostname(),
            execution_start_time=execution_start_time,
            execution_end_time=execution_end_time,
            task_start_time=task_start_time,
            task_end_time=task_end_time,
            input_transform_start_time=input_transform_start_time,
            input_transform_end_time=input_transform_end_time,
            result_transform_start_time=result_transform_start_time,
            result_transform_end_time=result_transform_end_time,
        )
        return _TaskResult(result, info)


class TaskFuture(Generic[T]):
    """Task future.

    Note:
        This class should not be instantiated by clients.

    Attributes:
        info: Task information and metadata.

    Args:
        future: Underlying future returned by the compute executor.
        info: Task information and metadata.
        data_transformer: Data transformer used to resolve the task result.
    """

    def __init__(
        self,
        future: Future[_TaskResult[T]],
        info: TaskInfo,
        data_transformer: TaskTransformer[Any],
    ) -> None:
        self.info = info
        self._future = future
        self._data_transformer = data_transformer

    def cancel(self) -> bool:
        """Attempt to cancel the task.

        If the call is currently being executed or finished running and
        cannot be cancelled then the method will return `False`, otherwise
        the call will be cancelled and the method will return `True`.
        """
        return self._future.cancel()

    def done(self) -> bool:
        """Return `True` is the call was successfully cancelled or finished."""
        return self._future.done()

    def exception(self) -> BaseException | None:
        """Get the exception raised by the task or `None` if successful."""
        return self._future.exception()

    def result(self, timeout: float | None = None) -> T:
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
        task_result = self._future.result(timeout=timeout)
        result = self._data_transformer.resolve(task_result.result)
        return result


def _result_or_cancel(
    future: TaskFuture[T],
    timeout: float | None = None,
) -> T:
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


class Engine:
    """Application execution engine.

    Args:
        executor: Task compute executor.
        data_filter: Data filter.
        data_transformer: Data transformer.
        record_logger: Task record logger.
    """

    def __init__(
        self,
        executor: Executor,
        *,
        data_filter: Filter | None = None,
        data_transformer: Transformer[Any] | None = None,
        record_logger: RecordLogger | None = None,
    ) -> None:
        self.executor = executor
        self.data_transformer: TaskTransformer[Any] = TaskTransformer(
            NullTransformer()
            if data_transformer is None
            else data_transformer,
            NullFilter() if data_filter is None else data_filter,
        )
        self.record_logger = (
            record_logger if record_logger is not None else NullRecordLogger()
        )

        # Maps user provided functions to the wrapped function.
        # This is tricky to type, so we just use Any.
        self._registered_tasks: dict[
            Callable[[Any], Any],
            _TaskWrapper[Any, Any],
        ] = {}

        # Internal bookkeeping
        self._running_tasks: dict[Future[Any], TaskFuture[Any]] = {}
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

    @property
    def tasks_executed(self) -> int:
        """Total number of tasks submitted for execution."""
        return self._total_tasks

    def _task_done_callback(self, future: Future[Any]) -> None:
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
        self.record_logger.log(dataclasses.asdict(task_future.info))

    # Note: args/kwargs are typed as Any rather than P.args/P.kwargs
    # because the inputs may be TaskFuture types which will get translated
    # into the correct types before invoking the function.
    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> TaskFuture[T]:
        """Schedule the callable to be executed.

        This function can also accept
        [`TaskFuture`][taps.engine.TaskFuture] objects as input
        to denote dependencies between a parent and this child task.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`TaskFuture`][taps.engine.TaskFuture] object representing the \
            result of the execution of the callable accessible via \
            [`TaskFuture.result()`][taps.engine.TaskFuture.result].
        """
        task_id = uuid.uuid4()

        if function not in self._registered_tasks:
            self._registered_tasks[function] = _TaskWrapper(
                function,
                task_id=task_id,
                data_transformer=self.data_transformer,
            )

        task = cast(
            Callable[P, _TaskResult[T]],
            self._registered_tasks[function],
        )

        parents = [
            str(arg.info.task_id)
            for arg in (*args, *kwargs.values())
            if isinstance(arg, TaskFuture)
        ]
        info = TaskInfo(
            task_id=str(task_id),
            function_name=function.__name__,
            parent_task_ids=parents,
            submit_time=time.time(),
        )

        # Extract executor futures from inside TaskFuture objects
        args = tuple(
            arg._future if isinstance(arg, TaskFuture) else arg for arg in args
        )
        kwargs = {
            k: v._future if isinstance(v, TaskFuture) else v
            for k, v in kwargs.items()
        }

        args = self.data_transformer.transform_iterable(args)
        kwargs = self.data_transformer.transform_mapping(kwargs)

        future = self.executor.submit(task, *args, **kwargs)
        self._total_tasks += 1

        task_future = TaskFuture(future, info, self.data_transformer)
        self._running_tasks[future] = task_future
        future.add_done_callback(self._task_done_callback)

        return task_future

    def map(
        self,
        function: Callable[P, T],
        *iterables: Iterable[P.args],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
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
        def _result_iterator() -> Generator[T, None, None]:
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
        self.data_transformer.close()
        self.record_logger.close()


def as_completed(
    tasks: Sequence[TaskFuture[T]],
    timeout: float | None = None,
) -> Generator[TaskFuture[T], None, None]:
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
    futures = {task._future: task for task in tasks}

    kwargs = {'timeout': timeout}
    if len(tasks) == 0 or isinstance(tasks[0]._future, Future):
        _as_completed = as_completed_python
    elif isinstance(tasks[0]._future, DaskFuture):
        _as_completed = as_completed_dask
        if sys.version_info < (3, 9):  # pragma: <3.9 cover
            kwargs = {}
    else:  # pragma: no cover
        raise ValueError(f'Unsupported future type {type(tasks[0])}.')

    for completed in _as_completed(futures.keys(), **kwargs):
        yield futures[completed]


def wait(
    tasks: Sequence[TaskFuture[T]],
    timeout: float | None = None,
    return_when: str = 'ALL_COMPLETED',
) -> tuple[set[TaskFuture[T]], set[TaskFuture[T]]]:
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
    futures = {task._future: task for task in tasks}

    if len(tasks) == 0 or isinstance(tasks[0]._future, Future):
        _wait = wait_python
    elif isinstance(tasks[0]._future, DaskFuture):
        _wait = wait_dask
    else:  # pragma: no cover
        raise ValueError(f'Unsupported future type {type(tasks[0])}.')

    completed_futures, not_completed_futures = _wait(
        list(futures.keys()),
        timeout=timeout,
        return_when=return_when,
    )

    completed_tasks = {futures[f] for f in completed_futures}
    not_completed_tasks = {futures[f] for f in not_completed_futures}

    return (completed_tasks, not_completed_tasks)
