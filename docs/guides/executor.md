# Task Executors

The [`Engine`][taps.engine.Engine], in TaPS, wraps a *task executor*.
Task executors are responsible for managing the asynchronous execution of functions.
Task executors implement Python's [`concurrent.futures.Executor`][concurrent.futures.Executor] model, and TaPS supports an extensible plugin system for configuring executor parameters and adding new executor types.

The rest of this guide describes creating a new executor within the TaPS framework.

## Creating an Executor

Here, we will create a `SyncExecutor` which simply executes a function directly.
This is not a very useful executor in practice as it does not enable an concurrency, but it will suffice for explaining the steps.

!!! note

    This step can be skipped if you already have an implementation that implements the [`concurrent.futures.Executor`][concurrent.futures.Executor] model.
    This step is for when (a) you are implementing an executor from scratch or (b) you need to wrap an existing executor with a [`concurrent.futures.Executor`][concurrent.futures.Executor] compliant interface.

The below code in `taps/executor/sync.py` implements the required `submit()`, `map()`, and `shutdown()` methods of `SyncExecutor`.
Note that [`concurrent.futures.Executor`][concurrent.futures.Executor] provides a default implementation of `map()` which can be suitable if the implementation does not have a special mechanism for handling mapped tasks.

```python title="taps/executor/sync.py" linenums="1"
from __future__ import annotations

import sys
import time
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

P = ParamSpec('P')
T = TypeVar('T')


class SyncExecutor(Executor):
    """Synchronous execution engine.

    Args:
        sleep: Time to sleep before executing tasks.
    """

    def __init__(self, sleep: float = 0) -> None:
        self.sleep = sleep

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`Future`][concurrent.futures.Future]-like object representing \
            the result of the execution of the callable.
        """
        future: Future[T] = Future()
        time.sleep(self.sleep)
        future.set_result(function(*args, **kwargs))
        return future

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
            chunksize: Sets the Dask batch size.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.
        """
        # Many implementation may choose to implement a better optimized
        # map(), but concurrent.futures.Executor does provide a map()
        # implementation which just calls submit() on each iterable.
        return super().map(
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
        """Shutdown the client."""
        pass
```

## Creating a Config

Config classes are how plugins are registered within TaPS.
For executors, every config must inherit from [`ExecutorConfig`][taps.executor.ExecutorConfig], an [abstract base class][abc.ABC] with an abstract method `get_executor()`.

The [`@register('executor')`][taps.plugins.register] decorator registers the config as a new executor plugin.
Registering the plugin makes our `SyncExecutor` available as an option with the CLI and enables input validation on fields of our executor.

```python title="taps/executor/sync.py" linenums="1" hl_lines="10 18 20-22 102-118"
from __future__ import annotations

import sys
import time
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import Literal
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from pydantic import Field

from taps.executor import ExecutorConfig
from taps.executor.utils import FutureDependencyExecutor
from taps.plugins import register

P = ParamSpec('P')
T = TypeVar('T')


class SyncExecutor(Executor):
    """Synchronous execution engine.

    Args:
        sleep: Time to sleep before executing tasks.
    """

    def __init__(self, sleep: float = 0) -> None:
        self.sleep = sleep

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`Future`][concurrent.futures.Future]-like object representing \
            the result of the execution of the callable.
        """
        future: Future[T] = Future()
        time.sleep(self.sleep)
        future.set_result(function(*args, **kwargs))
        return future

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
            chunksize: Sets the Dask batch size.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.
        """
        # Many implementation may choose to implement a better optimized
        # map(), but concurrent.futures.Executor does provide a map()
        # implementation which just calls submit() on each iterable.
        return super().map(
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
        """Shutdown the client."""
        pass


@register('executor')
class SyncExecutorConfig(ExecutorConfig):
    """Synchronous executor configuration."""

    name: Literal['sync'] = Field('sync', description='Executor name.')
    sleep: float = Field(
        0,
        description='Time to sleep before executing tasks.',
    )

    def get_executor(self) -> FutureDependencyExecutor:
        """Create an executor instance from the config."""
        return FutureDependencyExecutor(SyncExecutor(sleep=self.sleep))
```

The changes necessary to add the config to `taps/executor/sync.py` are highlighted.
The `name` field of `SyncExecutorConfig` defines the name via which this executor can be selected from the run CLI.

!!! warning

    The [`Engine`][taps.engine.Engine] requires that task executors support implicit data flow dependencies between tasks with futures.
    In other words, this means that it must be possible to pass the future from one task as a positional or keyword argument to another task.
    Many executors already support this (e.g., Dask or Parsl), but many do not (e.g., Python's [`ThreadPoolExecutor`][concurrent.futures.ThreadPoolExecutor] and [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor].

    TaPS provides the [`FutureDependencyExecutor`][taps.executor.utils.FutureDependencyExecutor] which can wrap another [`Executor`][concurrent.futures.Executor] instance to enable implicit data flow dependencies.
    Since `SyncExecutor.submit()` does not support accepting a `Future` in place of a positional or keyword argument, we must wrap the `SyncExecutor` in a [`FutureDependencyExecutor`][taps.executor.utils.FutureDependencyExecutor] in `SyncExecutorConfig.get_executor()`.

The last step is to import the `SyncExecutor` and `SyncExecutorConfig` inside of `taps/executor/__init__.py`.
This ensures that the [`@register`][taps.plugins.register] decorators get executed.

```python title="taps/executor/__init__.py" hl_lines="3-4"
...
from taps.executor.python import ThreadPoolConfig
from taps.executor.sync import SyncExecutor
from taps.executor.sync import SyncExecutorConfig
from taps.executor.ray import RayConfig
...
```

## Using the Executor

Now that we have created our `SyncExecutor` and registered the corresponding `SyncExecutorConfig`, we can utilize the executor to perform an benchmark.
```python
python -m taps.run --app cholesky --app.matrix-size 100 --app.block-size 25 \
    --engine.executor sync --engine.executor.sleep 0.1
```
The executor is available under the name `sync`, and we can also see that the `sleep` field is available as an optional CLI parameter since we gave it a default value.

```bash
[2024-07-10 13:36:08.774] RUN   (taps.run) :: Starting app (name=cholesky)
[2024-07-10 13:36:08.774] RUN   (taps.run) :: Configuration:
app:
  name: 'cholesky'
  block_size: 25
  matrix_size: 100
engine:
  executor:
    name: 'sync'
    sleep: 0.1
  filter:
    name: 'null'
  task_record_file_name: 'tasks.jsonl'
  transformer:
    name: 'null'
logging:
  file_level: 'INFO'
  file_name: 'log.txt'
  level: 'INFO'
run:
  dir_format: 'runs/{name}_{executor}_{timestamp}'
[2024-07-10 13:36:08.774] RUN   (taps.run) :: Runtime directory: runs/cholesky_sync_2024-07-10-13-36-08
[2024-07-10 13:36:08.774] APP   (taps.apps.cholesky) :: Generated input matrix: (100, 100)
[2024-07-10 13:36:08.775] APP   (taps.apps.cholesky) :: Block size: 25
[2024-07-10 13:36:11.953] APP   (taps.apps.cholesky) :: Output matrix: (100, 100)
[2024-07-10 13:36:11.953] RUN   (taps.run) :: Finished app (name=cholesky, runtime=3.18s, tasks=30)
```
As expected, the application took just over 3s to run since there were 30 tasks and we added a 0.1s sleep to each task inside our custom executor.
