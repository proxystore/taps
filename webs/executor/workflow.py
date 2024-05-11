from __future__ import annotations

import sys
import uuid
from concurrent.futures import Executor
from concurrent.futures import Future
from types import TracebackType
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

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.data.transform import NullTransformer
from webs.data.transform import TaskDataTransformer

P = ParamSpec('P')
T = TypeVar('T')


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

    def __call__(self, *args: Any, **kwargs: Any) -> T:
        """Call the function associated with the task."""
        args = self.data_transformer.resolve_iterable(args)
        kwargs = self.data_transformer.resolve_mapping(kwargs)
        return self.function(*args, **kwargs)


class WorkflowTask(Generic[T]):
    """Workflow task information."""

    def __init__(self, future: Future[T], task_id: uuid.UUID) -> None:
        self.future = future
        self.task_id = task_id


class WorkflowExecutor:
    """Workflow executor.

    Args:
        compute_executor: Compute executor.
    """

    def __init__(
        self,
        compute_executor: Executor,
        data_transformer: TaskDataTransformer[Any] | None = None,
    ) -> None:
        self.compute_executor = compute_executor
        self.data_transformer = (
            data_transformer
            if data_transformer is not None
            else TaskDataTransformer(NullTransformer())
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.shutdown()

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> WorkflowTask[T]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`WorkflowTask`][webs.executor.workflow.WorkflowTask`] object \
            containing the [`Future`][concurrent.futures.Future] object \
            representing the result of the execution of the callable.
        """
        task_id = uuid.uuid4()
        task = _TaskWrapper(
            function,
            task_id=task_id,
            data_transformer=self.data_transformer,
        )

        args = self.data_transformer.transform_iterable(args)
        kwargs = self.data_transformer.transform_mapping(kwargs)

        future = self.compute_executor.submit(task, *args, **kwargs)
        return WorkflowTask(future, task_id)

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
