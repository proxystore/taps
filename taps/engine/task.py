from __future__ import annotations

import dataclasses
import functools
import socket
import sys
import time
from dataclasses import field
from typing import Any
from typing import Callable
from typing import Generic
from typing import List
from typing import Optional
from typing import overload
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from taps.engine.transform import TaskTransformer

P = ParamSpec('P')
R = TypeVar('R')


@dataclasses.dataclass(frozen=True)
class ExceptionInfo:
    """Task exception information."""

    type: str = field(metadata={'description': 'Exception type.'})
    message: str = field(metadata={'description': 'Exception message.'})
    traceback: str = field(metadata={'description': 'Exception traceback.'})


@dataclasses.dataclass(frozen=True)
class ExecutionInfo:
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

    hostname: str = field(
        metadata={'description': 'Name of the host the task was executed on.'},
    )
    execution_start_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the task began execution on a '
                'worker.'
            ),
        },
    )
    execution_end_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the task finished execution '
                'on a worker.'
            ),
        },
    )
    task_start_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the start of execution of '
                'the task function.'
            ),
        },
    )
    task_end_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the end of execution of '
                'the task function.'
            ),
        },
    )
    input_transform_start_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the start of resolving input '
                'arguments on the worker.'
            ),
        },
    )
    input_transform_end_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the end of resolving input '
                'arguments on the worker.'
            ),
        },
    )
    result_transform_start_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the start of transforming the '
                'task function result on the worker.'
            ),
        },
    )
    result_transform_end_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the end of transforming the '
                'task function result on the worker.'
            ),
        },
    )


@dataclasses.dataclass()
class TaskInfo:
    """Task execution information."""

    task_id: str = field(
        metadata={
            'description': (
                'Unique UUID of the task as determined by the engine.'
            ),
        },
    )
    name: str = field(
        metadata={
            'description': (
                'Name of the task. Typically defaults to the name of the '
                'function unless overridden.'
            ),
        },
    )
    parent_task_ids: List[str] = field(  # noqa: UP006
        metadata={
            'description': (
                'UUIDs of parent tasks. A task is a child task if its '
                'arguments contain a future to the result of another task.'
            ),
        },
    )
    submit_time: float = field(
        metadata={
            'description': (
                'Unix timestamp indicating the engine submitted the task '
                'to the executor.'
            ),
        },
    )
    received_time: Optional[float] = field(  # noqa: UP045
        default=None,
        metadata={
            'description': (
                'Unix timestamp indicating the executor was notified the task '
                'has completed. This is recorded in a callback on the task '
                'future and thus includes any lag in invoking the future '
                'callbacks.'
            ),
        },
    )
    success: Optional[bool] = field(  # noqa: UP045
        default=None,
        metadata={
            'description': (
                'Boolean indicating if the task completed without raising '
                'an exception.'
            ),
        },
    )
    exception: Optional[ExceptionInfo] = field(  # noqa: UP045
        default=None,
        metadata={'description': 'Task exception information.'},
    )
    execution: Optional[ExecutionInfo] = field(  # noqa: UP045
        default=None,
        metadata={'description': 'Task execution information.'},
    )

    def asdict(self) -> dict[str, Any]:
        """Get task info as a dictionary."""
        return dataclasses.asdict(self)


class TaskResult(Generic[R]):
    """Task result structure.

    Args:
        value: The result of the task's function.
        info: Task execution information.
    """

    def __init__(self, value: R, info: ExecutionInfo) -> None:
        self.value = value
        self.info = info


@runtime_checkable
class Task(Generic[P, R], Protocol):
    """Task protocol for a wrapped function.

    Note:
        This is just a [`Protocol`][typing.Protocol] to define the behavior
        of a task, which is ultimately just a wrapper around a function.

    A task can be created with the [`@task()`][taps.engine.task.task]
    decorator. A task has different behavior based on if it is being called
    directly or as a task executed by the [`Engine`][taps.engine.Engine].

      * When called directly, the value of `_transformer` defaults to `None`,
        so the wrapped function is just invoked directly and the result
        returned. Here, the return type is `R`.
      * When executed by the [`Engine`][taps.engine.Engine], `_transformer`
        is not `None`, so the wrapped function will be executed with the
        additional task management. Here, the return type is
        [`TaskResult[R]`][taps.engine.task.TaskResult].

    Attributes:
        name: Name of the task used for logging.
    """

    name: str
    __wrapped__: Callable[P, R]

    @overload
    def __call__(
        self,
        *args: Any,
        _transformer: None = None,
        **kwargs: Any,
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
        *args: Any,
        _transformer: TaskTransformer[Any] | None = None,
        **kwargs: Any,
    ) -> TaskResult[R] | R:
        """Execute the task or wrapped function.

        Args:
            args: Positional arguments to pass to the wrapped function.
            _transformer: Transformer to use when resolving task arguments and
                transforming task results. This should never be provided
                by user code; this is only used when submitted as a task
                by the [`Engine`][taps.engine.Engine].
            kwargs: Keyword arguments to pass to the wrapped function.

        Returns:
            The result of type `R` from the wrapped function, possible \
            wrapped in a [`TaskResult[R]`][taps.engine.task.TaskResult] if \
            invoked by the [`Engine`][taps.engine.Engine].
        """
        ...


def _execute(
    function: Callable[P, R],
    *args: Any,
    _transformer: TaskTransformer[Any] | None = None,
    **kwargs: Any,
) -> TaskResult[R] | R:
    if _transformer is None:
        return function(*args, **kwargs)

    return _execute_task(function, *args, **kwargs, _transformer=_transformer)


def _execute_task(
    function: Callable[P, R],
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
    result = function(*args, **kwargs)
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
def task(
    function: Callable[P, R],
    *,
    name: str | None = None,
    wrap: bool | None = None,
) -> Task[P, R]: ...


@overload
def task(
    function: None = None,
    *,
    name: str | None = None,
    wrap: bool | None = None,
) -> Callable[[Callable[P, R]], Task[P, R]]: ...


def task(
    function: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    wrap: bool | None = None,
) -> Callable[[Callable[P, R]], Task[P, R]] | Task[P, R]:
    """Turn a *function* in a *task*.

    A task represents a wrapped, callable object (e.g., a function) that is
    executed on a worker by the [`Engine`][taps.engine.Engine]. This wrapper
    will perform additional task management, such as recording metrics and
    transforming task parameters and return values.

    Note:
        For convenience, this decorator/function is re-exported in
        [`taps.engine`][taps.engine].

    Note:
        The wrapped function can still be invoked directly, such as within unit
        tests or when *not* submitted to the [`Engine`][taps.engine.Engine].
        In this case, none of the additional task management is performed.

    Tip:
        Decorating top-level functions that will be submitted to the
        [`Engine`][taps.engine.Engine] by an application with the `@task`
        decorator is generally recommended.

    Example:
        Use as a decorator (parenthesis are required).
        ```python
        from taps.engine import task

        @task()
        def foo(*args, **kwargs) -> ...:
            ...
        ```
        Create a new task from a function:
        ```python
        from taps.engine import task

        def foo(*args, **kwargs) -> ...:
            ...

        foo_task = task(foo)
        ```

    Failure:
       The following pickling errors may occur in certain scenarios.

         * ```
           PicklingError: Can't pickle <function ...>: it's not the same object as <...>
           ```
         * ```
           AttributeError: Can't pickle local object 'task.<locals>.wrapped'
           ```

        If using `task()` as a decorator, ensure you are *calling* the
        decorator (e.g., with parenthesis).
        ```python
        @task()
        def foo() -> None: pass
        ```

        Otherwise, try changing the value of the `wrap` argument.

    Args:
        function: Optional function to wrap. If not provided, this function
            acts like a decorator factory, returning a new callable.
        name: Optional name for the task. Defaults to the `__name__` of the
            wrapped function.
        wrap: Mutate the wrapper function to looked like the wrapped function
            using [`functools.wraps()`][functools.wraps]. If `False`,
            a [`functools.partial`][functools.partial] function is
            returned instead. The default value `None` attempts to infer the
            best choice based on used: `#!python wrap=True` when used as a
            decorator (e.g., `#!python @task()`) and `#!python wrap=False`
            when used to directly create a task
            (e.g., `#!python foo_task = task(foo)`).
    """  # noqa: E501
    if function is None:
        # The function was called as a decorator factory so return a new
        # callable that can function as a decorator. In this case, we are
        # replacing the decorated function so we default to wrap=True so
        # pickling by reference works.
        return functools.partial(  # type: ignore[return-value]
            task,
            name=name,
            wrap=True if wrap is None else wrap,
        )

    name = name if name is not None else function.__name__
    # If this function was invoked directly with the function to wrap
    # passed as a parameter, we default wrap=False so the returned type
    # is a partial function which has special support for pickling.
    wrap = False if wrap is None else wrap

    if wrap:
        # Using `functools.wraps` updates `wrapper` to look like `function`.
        # This has implications when `wrapper` is pickled as its `__name__`
        # and `__module__` will be the same as `function`. It will only work
        # if `wrapper` is *replacing* `function` in the module because
        # this was used as a decorator around `function`. In contrast,
        # using task as a function and storing the result to a new variable
        # will not be pickleable (e.g., `foo_task = task(foo, wrap=True)`).
        @functools.wraps(function)
        def wrapper(
            *args: Any,
            _transformer: TaskTransformer[Any] | None = None,
            **kwargs: Any,
        ) -> TaskResult[R] | R:
            return _execute(
                function,
                *args,
                **kwargs,
                _transformer=_transformer,
            )

        wrapper.__dict__['name'] = name
        return wrapper  # type: ignore[return-value]

    # Using `functools.partial` creates a new callable object (an instance
    # of the `partial` class) that references `function`.
    wrapped = functools.partial(_execute, function)
    wrapped.__dict__['name'] = name
    wrapped.__dict__['__wrapped__'] = function
    return wrapped  # type: ignore[return-value]
