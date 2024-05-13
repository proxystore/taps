from __future__ import annotations

import dataclasses
import sys
import time
import uuid
from concurrent.futures import Executor
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generator
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import NamedTuple
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.data.transform import NullTransformer
from webs.data.transform import TaskDataTransformer
from webs.record import NullRecordLogger
from webs.record import RecordLogger

P = ParamSpec('P')
T = TypeVar('T')


class _TaskResult(NamedTuple, Generic[T]):
    result: T


class _TaskWrapper(Generic[P, T]):
    """Workflow task wrapper.

    Args:
        function: Function that represents the work associated with the task.
        task_id: Unique UUID of the task.
    """

    def __init__(
        self,
        function: Callable[P, T],
        *,
        task_id: uuid.UUID,
        data_transformer: TaskDataTransformer[Any],
    ) -> None:
        self.function = function
        self.task_id = uuid.uuid4() if task_id is None else task_id
        self.data_transformer = data_transformer

    def __call__(self, *args: Any, **kwargs: Any) -> _TaskResult[T]:
        """Call the function associated with the task."""
        args = tuple(
            arg.result if isinstance(arg, _TaskResult) else arg for arg in args
        )
        kwargs = {
            k: v.result if isinstance(v, _TaskResult) else v
            for k, v in kwargs.items()
        }
        args = self.data_transformer.resolve_iterable(args)
        kwargs = self.data_transformer.resolve_mapping(kwargs)
        result = self.function(*args, **kwargs)
        return _TaskResult(result)


@dataclasses.dataclass
class _TaskInfo:
    task_id: str
    function_name: str
    parent_task_ids: list[str]
    started_time: float
    finished_time: float | None = None


class TaskFuture(Generic[T]):
    """Workflow task future.

    Note:
        This class should not be instantiated by clients.

    Attributes:
        info: Task information and metadata.

    Args:
        future: Underlying future returned by the compute executor.
        info: Task information and metadata.
    """

    def __init__(
        self,
        future: Future[_TaskResult[T]],
        info: _TaskInfo,
    ) -> None:
        self.info = info
        self._future = future

    def cancel(self) -> bool:
        """Attempt to cancel the task.

        If the call is currently being executed or finished running and
        cannot be cancelled then the method will return `False`, otherwise
        the call will be cancelled and the method will return `True`.
        """
        return self._future.cancel()

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
        return self._future.result(timeout=timeout).result


def _result_or_cancel(
    future: TaskFuture[T],
    timeout: float | None = None,
) -> T:
    try:
        try:
            return future.result(timeout)
        finally:
            future.cancel()
    finally:
        # Break a reference cycle with the exception in self._exception
        del future


class WorkflowExecutor:
    """Workflow executor.

    Args:
        compute_executor: Compute executor.
    """

    def __init__(
        self,
        compute_executor: Executor,
        *,
        data_transformer: TaskDataTransformer[Any] | None = None,
        record_logger: RecordLogger | None = None,
    ) -> None:
        self.compute_executor = compute_executor
        self.data_transformer = (
            data_transformer
            if data_transformer is not None
            else TaskDataTransformer(NullTransformer())
        )
        self.record_logger = (
            record_logger if record_logger is not None else NullRecordLogger()
        )

        # Internal bookkeeping
        self._running_tasks: dict[Future[Any], TaskFuture[Any]] = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.shutdown()

    def _task_done_callback(self, future: Future[Any]) -> None:
        task_future = self._running_tasks.pop(future)
        task_future.info.finished_time = time.time()
        self.record_logger.log(dataclasses.asdict(task_future.info))

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> TaskFuture[T]:
        """Schedule the callable to be executed.

        This function can also accept
        [`WorkflowTask`][webs.executor.workflow.WorkflowTask] objects as input
        to denote dependencies between a parent and this child task.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`TaskFuture`][webs.executor.workflow.TaskFuture`] object \
            representing the result of the execution of the callable
            accessible via \
            [`TaskFuture.result()`][webs.execution.workflow.TaskFuture.result].
        """
        task_id = uuid.uuid4()
        task = _TaskWrapper(
            function,
            task_id=task_id,
            data_transformer=self.data_transformer,
        )

        parents = [
            str(arg.info.task_id)
            for arg in (*args, *kwargs.values())
            if isinstance(arg, TaskFuture)
        ]
        info = _TaskInfo(
            task_id=str(task_id),
            function_name=function.__name__,
            parent_task_ids=parents,
            started_time=time.time(),
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

        future = self.compute_executor.submit(task, *args, **kwargs)

        task_future = TaskFuture(future, info)
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
            try:
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
            finally:  # pragma: no cover
                for task in tasks:
                    task.cancel()

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
            self.compute_executor.shutdown(
                wait=wait,
                cancel_futures=cancel_futures,
            )
        else:  # pragma: <3.9 cover
            self.compute_executor.shutdown(wait=wait)
