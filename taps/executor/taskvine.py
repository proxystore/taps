from __future__ import annotations

import inspect
import logging
import sys
import time
from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import Literal
from typing import Optional
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from pydantic import Field

try:  # pragma: no cover
    from ndcctools.taskvine import FuturesExecutor
    from ndcctools.taskvine import VineFuture

    taskvine_import_error: Exception | None = None
except ImportError as e:  # pragma: no cover
    FuturesExecutor = object
    taskvine_import_error = e

import taps.apps
import taps.engine
import taps.executor
import taps.logging
import taps.transformer
from taps.executor import ExecutorConfig
from taps.plugins import register

P = ParamSpec('P')
R = TypeVar('R')

logger = logging.getLogger(__name__)


# Note: this is not type annotate because otherwise the TaskVine library
# needs the type annotations to be imported as well.
def _wrap(task, *args, **kwargs):  # type: ignore[no-untyped-def]  # pragma: no cover
    return task(*args, **kwargs)


class TaskVineExecutor(FuturesExecutor):  # pragma: no cover
    """TaskVine executor.

    Extends TaskVine's `FuturesExecutor` to enable support for serverless
    mode.

    Warning:
        The [CCTools](https://github.com/cooperative-computing-lab/cctools)
        package is not installed with TaPS. We recommend installing both
        TaPS and CCTools into a Conda environment. See the
        [CCTools installation guide](https://cctools.readthedocs.io/en/stable/install/)
        for all options. Version 7.13.2 and later of `ndcctools` is required.

    Args:
        args: Positional arguments to pass to `FuturesExecutor`.
        cores_per_task: Number of cores required by each task.
        serverless: Enable serverless mode to preload libraries on workers.
        debug: Enable additional TaskVine logging.
        kwargs: Keyword arguments to pass to `FuturesExecutor`.

    Raises:
        ImportError: If `cctools` is not installed.
    """

    def __init__(
        self,
        *args: Any,
        cores_per_task: int,
        serverless: bool,
        debug: bool = False,
        **kwargs: Any,
    ) -> None:
        if taskvine_import_error is not None:
            raise taskvine_import_error

        super().__init__(*args, **kwargs)
        self.lib_installed: set[str] = set()
        self.cores_per_task = cores_per_task
        self.worker_cores = kwargs['opts'].get('cores', None)
        self.serverless = serverless

        if debug:
            self.manager.tune('watch-library-logfiles', 1)

    def _submit_serverless(
        self,
        function: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> VineFuture[R]:
        func_mod = inspect.getmodule(function)
        if func_mod is None:
            raise ValueError(
                f'Unable to determine module of task function {function}.',
            )
        lib_name = f'{func_mod.__name__}-taskvine-lib'
        if lib_name not in self.lib_installed:
            modules = [
                # This module
                sys.modules[__name__],
                # Common taps modules
                taps.apps,
                taps.engine,
                taps.executor,
                taps.logging,
                taps.transformer,
                # The module containing the function
                func_mod,
            ]
            lib = self.create_library_from_functions(
                lib_name,
                _wrap,
                add_env=False,
                hoisting_modules=modules,
            )
            if self.worker_cores is not None:
                lib.set_cores(self.worker_cores)
                lib.set_function_slots(
                    self.worker_cores // self.cores_per_task,
                )
            self.install_library(lib)
            self.lib_installed.add(lib_name)
            logger.debug(
                f'Installed library on TaskVine workers (name={lib_name})',
            )

        # Note: We submit a wrapper function as the "task" and the actual
        # function as an argument because tasks in TaPS are decorated by
        # @task() and TaskVine libraries do not support statically
        # decorated top-level functions.
        fn = self.future_funcall(
            lib_name,
            '_wrap',
            function,
            *args,
            **kwargs,
        )
        fn.set_cores(self.cores_per_task)
        return super().submit(fn)

    def submit(
        self,
        function: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> VineFuture[R]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`Future`][concurrent.futures.Future]-like object representing \
            the result of the execution of the callable.
        """
        if self.serverless:
            return self._submit_serverless(function, *args, **kwargs)
        else:
            fn = self.future_task(function, *args, **kwargs)
            fn.set_cores(self.cores_per_task)
            self.task_table.append(fn)
            return super().submit(fn, *args, **kwargs)

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
            chunksize: Sets the Dask batch size.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.
        """
        # This is a modified version of Executor.map() that does not call
        # cancel() on the futures if a future raises an exception. TaskVine's
        # VineFuture.cancel() has an attribute lookup bug when using a
        # FutureFunctionCall.
        if timeout is not None:
            end_time = timeout + time.monotonic()

        futures = [self.submit(function, *args) for args in zip(*iterables)]

        def _result_iterator() -> Generator[R, None, None]:
            futures.reverse()
            while futures:
                if timeout is None:
                    yield futures.pop().result(timeout)
                else:
                    yield futures.pop().result(end_time - time.monotonic())

        return _result_iterator()


@register('executor')
class TaskVineConfig(ExecutorConfig):
    """TaskVine executor plugin configuration."""

    name: Literal['taskvine'] = Field('taskvine', description='Executor name.')
    cores_per_task: int = Field(
        1,
        description='Number of cores per task.',
    )
    cores_per_worker: Optional[int] = Field(  # noqa: UP007
        None,
        description='Number of cores per worker.',
    )
    debug: bool = Field(
        False,
        description='Enable additional TaskVine logging.',
    )
    factory: bool = Field(
        True,
        description='Launch workers from a factory.',
    )
    port: int = Field(
        9123,
        description='Port of TaskVine manager.',
    )
    serverless: bool = Field(
        False,
        description='Use TaskVine serverless mode.',
    )
    workers: int = Field(
        1,
        description='TaskVine workers when using a factory.',
    )

    def get_executor(self) -> FuturesExecutor:
        """Create an executor instance from the config."""
        opts: dict[str, Any] = {
            'min_workers': self.workers,
            'max_workers': self.workers,
        }
        if self.cores_per_worker is not None:
            opts['cores'] = self.cores_per_worker

        return TaskVineExecutor(
            manager_name='taps-taskvine-manager',
            port=self.port,
            opts=opts,
            cores_per_task=self.cores_per_task,
            debug=self.debug,
            factory=self.factory,
            serverless=self.serverless,
        )
