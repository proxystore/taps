from __future__ import annotations

import dataclasses
import functools
import socket
import sys
import time
from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from taps.engine.transform import TaskTransformer

P = ParamSpec('P')
T = TypeVar('T')


@dataclasses.dataclass
class ExceptionInfo:
    """Task exception information.

    Args:
        type: Type of the raised exception.
        message: Message of the raised exception.
        traceback: Trackback of the raised exception.
    """

    type: str
    message: str
    traceback: str


@dataclasses.dataclass
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

    Args:
        hostname: Name of the host the task was executed on.
        execution_start_time: Unix timestamp indicating the task began
            execution on a worker.
        execution_end_time: Unix timestamp indicating the task finished
            execution on a worker.
        task_start_time: Unix timestamp indicating the start of execution
            of the task function.
        task_end_time: Unix timestamp indicating the end of execution
            of the task function.
        input_transform_start_time: Unix timestamp indicating the start
            of resolving input arguments on the worker.
        input_transform_end_time: Unix timestamp indicating the end
            of resolving input arguments on the worker.
        result_transform_start_time: Unix timestamp indicating the start
            of transforming the task function result on the worker.
        result_transform_end_time: Unix timestamp indicating the end
            of transforming the task function result on the worker.
    """

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
    """Task execution information.

    Args:
        task_id: Unique UUID of the task as determined by the engine.
        function_name: Name of the task function.
        parent_task_ids: UUIDs of parent tasks. A task is a child task if its
            arguments contain a future to the result of another task.
        submit_time: Unix timestamp indicating the engine submitted the task
            to the executor.
        received_time: Unix timestamp indicating the executor was notified the
            task has completed. This is recorded in a callback on the task
            future and thus includes any lag in invoking the future callbacks.
        success: Boolean indicating if the task completed without raising an
            exception.
        exception: Task exception information.
        execution: Task execution information.
    """

    task_id: str
    function_name: str
    parent_task_ids: list[str]
    submit_time: float
    received_time: float | None = None
    success: bool | None = None
    exception: ExceptionInfo | None = None
    execution: ExecutionInfo | None = None


@dataclasses.dataclass
class TaskResult(Generic[T]):
    """Task result structure.

    Args:
        result: The result of the task's function.
        info: Task execution information.
    """

    result: T
    info: ExecutionInfo


class Task(Generic[P, T]):
    """Task wrapper.

    The task wrapper is what is actually invoked on a worker. The wrapper is
    a callable object that will perform metric recording and data transformatin
    before and after invoking the task function.

    Args:
        function: Function that represents the work associated with the task.
        transformer: Transformer to use when resolving task arguments and
            transforming task results.
    """

    def __init__(
        self,
        function: Callable[P, T],
        *,
        transformer: TaskTransformer[Any],
    ) -> None:
        self.function = function
        self.transformer = transformer
        #  Make this class instance "look" like `function`.
        functools.update_wrapper(self, function)

    def __call__(self, *args: Any, **kwargs: Any) -> TaskResult[T]:
        """Call the function associated with the task."""
        execution_start_time = time.time()
        args = tuple(
            arg.result if isinstance(arg, TaskResult) else arg for arg in args
        )
        kwargs = {
            k: v.result if isinstance(v, TaskResult) else v
            for k, v in kwargs.items()
        }

        input_transform_start_time = time.time()
        args = self.transformer.resolve_iterable(args)
        kwargs = self.transformer.resolve_mapping(kwargs)
        input_transform_end_time = time.time()

        task_start_time = time.time()
        result = self.function(*args, **kwargs)
        task_end_time = time.time()

        result_transform_start_time = time.time()
        result = self.transformer.transform(result)
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
        return TaskResult(result, info)
