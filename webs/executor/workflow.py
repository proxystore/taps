from __future__ import annotations

import sys
import uuid
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

P = ParamSpec('P')
T = TypeVar('T')


class WorkflowTaskFuture(Future[T]):
    """Task result future.

    A thin wrapper around another future returned by the compute executor.
    Refer to the [`Future`][concurrent.futures.Future] docstring for
    further details on behaviour.

    Args:
        compute_future: Underlying future representing the result of the
            task execution.
        task_id: UUID of the task this future corresponds to.
    """

    def __init__(
        self,
        compute_future: Future[T],
        *,
        task_id: uuid.UUID,
    ) -> None:
        self.compute_future = compute_future
        self.task_id = task_id

    def cancel(self) -> bool:
        """Attempt to cancel the call."""
        return self.compute_future.cancel()

    def cancelled(self) -> bool:
        """Check if the call was successfully cancelled."""
        return self.compute_future.cancelled()

    def running(self) -> bool:
        """Check if the call is currently being executed."""
        return self.compute_future.running()

    def done(self) -> bool:
        """Check if the call was successfully cancelled or finished running."""
        return self.compute_future.done()

    def result(self, timeout: float | None = None) -> T:
        """Get the value returned by the call."""
        return self.compute_future.result(timeout)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Get the exception raised by the call."""
        return self.compute_future.exception(timeout)

    def add_done_callback(self, fn: Callable[[Future[T]], Any]) -> None:
        """Attach a callback for when the future is done."""
        self.compute_future.add_done_callback(fn)

    def set_running_or_notify_cancel(self) -> bool:
        """Set the future as running or notify the call was cancelled."""
        return self.compute_future.set_running_or_notify_cancel()

    def set_result(self, result: T) -> None:
        """Set the result of the work associated with the future."""
        self.compute_future.set_result(result)

    def set_exception(self, exception: BaseException | None) -> None:
        """Set the exception raised by the work associated with the future."""
        self.compute_future.set_exception(exception)


class WorkflowTask(Generic[P, T]):
    """Workflow task wrapper.

    Args:
        function: Function that represents the work associated with the task.
        task_id: Unique UUID of the task.
    """

    def __init__(
        self,
        function: Callable[P, T],
        *,
        task_id: uuid.UUID | None = None,
    ) -> None:
        self.function = function
        self.task_id = uuid.uuid4() if task_id is None else task_id

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Call the function associated with the task."""
        return self.function(*args, **kwargs)


class WorkflowExecutor(Executor):
    """Workflow executor.

    Args:
        compute_executor: Compute executor.
    """

    def __init__(self, compute_executor: Executor) -> None:
        self.compute_executor = compute_executor

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowTaskFuture[T]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`Future`][concurrent.futures.Future] object representing the \
            result of the execution of the callable.
        """
        task = WorkflowTask(function)
        compute_future = self.compute_executor.submit(task, *args, **kwargs)
        return WorkflowTaskFuture(compute_future, task_id=task.task_id)

    def map(
        self,
        function: Callable[P, T],
        *iterables: Iterable[P.args],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Map a function onto iterables of arguments.

        Note:
            This method simply calls `self.compute_executor.map()`.

        Args:
            function: A callable that will take as many arguments as there are
                passed iterables.
            iterables: Variable number of iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: If greater than one, the iterables will be chopped into
                chunks of size chunksize and submitted to the executor. If set
                to one, the items in the list will be sent one at a time.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.
        """
        return self.compute_executor.map(
            function,
            *iterables,
            timeout=timeout,
            chunksize=chunksize,
        )

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
