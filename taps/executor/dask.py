from __future__ import annotations

import sys
from concurrent.futures import Executor
from concurrent.futures import Future
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

import dask
from dask.distributed import Client
from dask.distributed import Future as DaskFuture
from pydantic import Field

from taps.executor import ExecutorConfig
from taps.plugins import register

P = ParamSpec('P')
T = TypeVar('T')


class DaskDistributedExecutor(Executor):
    """Dask task execution engine.

    Args:
        client: Dask distributed client.
    """

    def __init__(self, client: Client) -> None:
        self.client = client

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
        return self.client.submit(function, *args, **kwargs)

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
        # Based on the Parsl implementation.
        # https://github.com/Parsl/parsl/blob/7fba7d634ccade76618ee397d3c951c5cbf2cd49/parsl/concurrent/__init__.py#L58
        futures = self.client.map(
            function,
            *iterables,  # type: ignore[arg-type,unused-ignore]
            batch_size=chunksize,
        )

        def _result_iterator() -> Generator[T, None, None]:
            futures.reverse()
            while futures:
                yield futures.pop().result(timeout)

        return _result_iterator()

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the client."""
        if DaskFuture._cb_executor is not None:
            # Dask runs future callbacks in threads of a ThreadPoolExecutor
            # that is a class attributed of Dask's future. Shutting down
            # the client causes all futures to get cancelled, which can
            # cause a currently executing callback to raise a CancelledError
            # if the callback accesses the future's result.
            DaskFuture._cb_executor.shutdown(wait=wait)
            DaskFuture._cb_executor = None

        # Note: wait and cancel_futures are not implemented.
        self.client.close()


@register('executor')
class DaskDistributedConfig(ExecutorConfig):
    """[`DaskDistributedExecutor`][taps.executor.dask.DaskDistributedExecutor] plugin configuration.

    Attributes:
        scheduler: Dask scheduler address.
        use_threads: Use threads rather than processes for local clusters.
        workers: Number of Dask workers for local clusters.
        daemon_workers: Daemonize Dask workers.
    """  # noqa: E501

    name: Literal['dask'] = 'dask'
    scheduler: Optional[str] = Field(  # noqa: UP007
        None,
        description='dask scheduler address',
    )
    use_threads: bool = Field(
        False,
        description='use threads instead of processes for dask workers',
    )
    workers: Optional[int] = Field(  # noqa: UP007
        None,
        description='maximum number of dask workers',
    )
    daemon_workers: bool = Field(
        True,
        description='configure if workers are daemon',
    )

    def get_executor(self) -> DaskDistributedExecutor:
        """Create an executor instance from the config."""
        if self.scheduler is not None:
            client = Client(self.scheduler)
        else:
            dask.config.set(
                {'distributed.worker.daemon': self.daemon_workers},
            )
            client = Client(
                n_workers=self.workers,
                processes=not self.use_threads,
                dashboard_address=None,
            )
        return DaskDistributedExecutor(client)
