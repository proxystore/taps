from __future__ import annotations

import functools
import socket
import sys
import time
from typing import Any
from typing import Callable
from typing import Generic
from typing import List
from typing import Optional
from typing import overload
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from pydantic import BaseModel
from pydantic import Field

from taps.engine.transform import TaskTransformer

P = ParamSpec('P')
R = TypeVar('R')


class ExceptionInfo(BaseModel):
    """Task exception information."""

    type: str = Field(description='Exception type.')
    message: str = Field(description='Exception message.')
    traceback: str = Field(description='Exception traceback.')


class ExecutionInfo(BaseModel):
    """Task execution information.

    All times are Unix timestamps recorded on the worker process executing
    the task. The times are as follows:
    ```
    + execution_start_time
    |   + input_transform_start_time
    |   |   # Resolve task arguments
    |   + input_transform_end_time
    |
    |   + task_start_time
    |   |   # Execute task function
    |   + task_end_time
    |
    |   + result_transform_start_time
    |   |   # Transform task function result
    |   + result_transform_end_time
    + execution_end_time
    ```
    """

    hostname: str = Field(
        description='Name of the host the task was executed on.',
    )
    execution_start_time: float = Field(
        description=(
            'Unix timestamp indicating the task began execution on a worker.'
        ),
    )
    execution_end_time: float = Field(
        description=(
            'Unix timestamp indicating the task finished execution '
            'on a worker.'
        ),
    )
    task_start_time: float = Field(
        description=(
            'Unix timestamp indicating the start of execution of '
            'the task function.'
        ),
    )
    task_end_time: float = Field(
        description=(
            'Unix timestamp indicating the end of execution of '
            'the task function.'
        ),
    )
    input_transform_start_time: float = Field(
        description=(
            'Unix timestamp indicating the start of resolving input '
            'arguments on the worker.'
        ),
    )
    input_transform_end_time: float = Field(
        description=(
            'Unix timestamp indicating the end of resolving input '
            'arguments on the worker.'
        ),
    )
    result_transform_start_time: float = Field(
        description=(
            'Unix timestamp indicating the start of transforming the '
            'task function result on the worker.'
        ),
    )
    result_transform_end_time: float = Field(
        description=(
            'Unix timestamp indicating the end of transforming the '
            'task function result on the worker.'
        ),
    )


class TaskInfo(BaseModel):
    """Task execution information."""

    task_id: str = Field(
        description='Unique UUID of the task as determined by the engine.',
    )
    function_name: str = Field(description='Name of the task function.')
    parent_task_ids: List[str] = Field(  # noqa: UP006
        description=(
            'UUIDs of parent tasks. A task is a child task if its arguments '
            'contain a future to the result of another task.'
        ),
    )
    submit_time: float = Field(
        description=(
            'Unix timestamp indicating the engine submitted the task '
            'to the executor.'
        ),
    )
    received_time: Optional[float] = Field(  # noqa: UP007
        None,
        description=(
            'Unix timestamp indicating the executor was notified the task '
            'has completed. This is recorded in a callback on the task future '
            'and thus includes any lag in invoking the future callbacks.'
        ),
    )
    success: Optional[bool] = Field(  # noqa: UP007
        None,
        description=(
            'Boolean indicating if the task completed without raising '
            'an exception.'
        ),
    )
    exception: Optional[ExceptionInfo] = Field(  # noqa: UP007
        None,
        description='Task exception information.',
    )
    execution: Optional[ExecutionInfo] = Field(  # noqa: UP007
        None,
        description='Task execution information.',
    )


class TaskResult(Generic[R]):
    """Task result structure.

    Args:
        value: The result of the task's function.
        info: Task execution information.
    """

    def __init__(self, value: R, info: ExecutionInfo) -> None:
        self.value = value
        self.info = info


class Task(Generic[P, R]):
    """Task wrapper.

    A task represents a wrapped, callable object (e.g., a function) that is
    executed on a worker by the [`Engine`][taps.engine.Engine]. This wrapper
    will perform additional task management, such as recording metrics and
    transforming task parameters and return values.

    Note:
        A [`Task`][taps.engine.task.Task] is commonly created through the
        [`@task`][taps.engine.task.task] decorator. However, the decorated
        function can still be invoked directly, such as within unit tests
        or when *not* submitted to the [`Engine`][taps.engine.Engine].
        In this case, none of the additional task management is performed.

    Args:
        wrapped: Function representing the work associated with the task type.
    """

    # Set by functools.update_wrapper in __init__
    __module__: str
    __name__: str

    def __init__(self, wrapped: Callable[P, R]) -> None:
        self.wrapped = wrapped
        #  Make this class instance "look" like `wrapped`.
        functools.update_wrapper(self, wrapped)

    @overload
    def __call__(
        self,
        *args: P.args,
        _transformer: None = None,
        **kwargs: P.kwargs,
    ) -> R: ...

    @overload
    def __call__(
        self,
        *args: Any,
        _transformer: TaskTransformer[Any],
        **kwargs: Any,
    ) -> TaskResult[R]: ...

    def __call__(
        self,
        *args: P.args | Any,
        _transformer: TaskTransformer[Any] | None = None,
        **kwargs: P.kwargs | Any,
    ) -> TaskResult[R] | R:
        """Execute the task.

        This method has different behavior based on if it is being called
        directly or as a task executed by the [`Engine`][taps.engine.Engine].
        When called directly, the value of `_transformer` is `None`, so the
        task function (i.e., `self.wrapped`) is invoked and the result directly
        returned. Here, the return type is `R`.

        When executed by the [`Engine`][taps.engine.Engine], `_transformer`
        is not `None`, so the wrapped function will be executed with the
        additional task management. Here, the return type is
        [`TaskResult[R]`][taps.engine.task.TaskResult].

        Args:
            args: Positional arguments to pass to the wrapped function.
            _transformer: Transformer to use when resolving task arguments and
                transforming task results. This should never be provided
                by user code; this is only used when submitted as a task
                by the [`Engine`][taps.engine.Engine].
            kwargs: Keyword arguments to pass to the wrapped function.

        Returns:
            The result of type `R` from the wrapped function, possible wrapped
            in a [`TaskResult[R]`][taps.engine.task.TaskResult] if invoked
            by the [`Engine`][taps.engine.Engine].
        """
        if _transformer is None:
            return self.wrapped(*args, **kwargs)
        else:
            return self._call_as_task(
                *args,
                **kwargs,
                _transformer=_transformer,
            )

    def _call_as_task(
        self,
        *args: Any,
        _transformer: TaskTransformer[Any],
        **kwargs: Any,
    ) -> TaskResult[R]:
        execution_start_time = time.time()
        args = tuple(
            arg.value if isinstance(arg, TaskResult) else arg for arg in args
        )
        kwargs = {
            k: v.value if isinstance(v, TaskResult) else v
            for k, v in kwargs.items()
        }

        input_transform_start_time = time.time()
        args = _transformer.resolve_iterable(args)
        kwargs = _transformer.resolve_mapping(kwargs)
        input_transform_end_time = time.time()

        task_start_time = time.time()
        result = self.wrapped(*args, **kwargs)
        task_end_time = time.time()

        result_transform_start_time = time.time()
        result = _transformer.transform(result)
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
        return TaskResult(value=result, info=info)


@overload
def task(function: Callable[P, R], /) -> Task[P, R]: ...


@overload
def task(
    function: None = None,
    /,
) -> Callable[[Callable[P, R]], Task[P, R]]: ...


def task(
    function: Callable[P, R] | None = None,
    /,
) -> Task[P, R] | Callable[[Callable[P, R]], Task[P, R]]:
    """Decorator that converts a function into [`Task`][taps.engine.task.Task].

    Note:
        For convenience, this decorator is re-exported in
        [`taps.engine`][taps.engine].

    Tip:
        Decorating top-level functions that will be submitted to the
        [`Engine`][taps.engine.Engine] by an application with the `@task`
        decorator is generally recommended.

    Example:
        This decorator can be used directly:
        ```python
        from taps.engine import task

        @task
        def foo(*args, **kwargs) -> ...:
            ...
        ```
        Or by being called first:
        ```python
        from taps.engine import task

        @task()
        def foo(*args, **kwargs) -> ...:
            ...
        ```

    Args:
        function: Function to turn into a [`Task`][taps.engine.task.Task].
    """

    def decorator(function: Callable[P, R]) -> Task[P, R]:
        return Task(function)

    if function is None:
        return decorator
    else:
        return decorator(function)
