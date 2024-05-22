from __future__ import annotations

import sys
from concurrent.futures import Executor
from concurrent.futures import Future
from types import MethodType
from typing import Callable
from typing import cast
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import Optional
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from pydantic import Field
from ray.util.multiprocessing import Pool

from webs.executor.config import ExecutorConfig
from webs.executor.config import register

P = ParamSpec('P')
T = TypeVar('T')


def _return_single_item_result(
    self: Future[T],
    timeout: float | None = None,
) -> T:
    result = self.base_result()  # type: ignore[attr-defined]
    return result[0]


class RayPoolExecutor(Executor):
    """Ray distributed processing pool execution engine.

    Note:
        See the `ray.util.multiprocessing.Pool` documentation for full
        details of the arguments.

    Args:
        processes: Number of actor processes to start in the pool. Defaults to
            the number of cores in the Ray cluster, or the number of cores
            on this machine.
        ray_address: Address of the Ray cluster to run on. If `None`, starts
            a local cluster.
    """

    def __init__(
        self,
        processes: int | None = None,
        ray_address: str | None = None,
    ) -> None:
        self.pool = Pool(processes=processes, ray_address=ray_address)

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
        args = cast(
            P.args,
            tuple(
                arg.object_ref  # type: ignore[attr-defined]
                if isinstance(arg, Future)
                else arg
                for arg in args
            ),
        )
        kwargs = cast(
            P.kwargs,
            {
                k: v.object_ref if isinstance(v, Future) else v  # type: ignore[attr-defined]
                for k, v in kwargs.items()
            },
        )

        async_result = self.pool.apply_async(
            function,
            args=args,
            kwargs=kwargs,
        )
        assert len(async_result._result_thread._object_refs) == 1
        object_ref = async_result._result_thread._object_refs[0]
        future = object_ref.future()
        future.base_result = future.result
        future.result = MethodType(_return_single_item_result, future)
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
        # Based on the Parsl implementation.
        # https://github.com/Parsl/parsl/blob/7fba7d634ccade76618ee397d3c951c5cbf2cd49/parsl/concurrent/__init__.py#L58
        if len(iterables) > 1:
            raise ValueError('Multiple iterables are not supported.')
        iterable = iterables[0]
        iterable = tuple(
            v.object_ref  # type: ignore[attr-defined]
            if isinstance(v, Future)
            else v
            for v in iterable
        )

        async_result = self.pool.map_async(
            function,
            iterable=iterable,
            chunksize=chunksize,
        )
        futures = [
            object_ref.future()
            for object_ref in async_result._result_thread._object_refs
        ]

        for future in futures:
            future.base_result = future.result
            future.result = MethodType(_return_single_item_result, future)

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
        if wait:
            self.pool.close()
        else:
            self.pool.terminate()


@register(name='ray')
class RayPoolConfig(ExecutorConfig):
    """Ray distributed multiprocessing pool configuration.

    Attributes:
        ray_address: Address of the Ray cluster to run on. If `None`, starts
            a local cluster.
        processes: Number of actor processes to start in the pool. Defaults to
            the number of cores in the Ray cluster, or the number of cores
            on this machine.
    """

    ray_address: Optional[str] = Field(  # noqa: UP007
        None,
        description='ray scheduler address (default spawns local cluster)',
    )
    ray_processes: Optional[int] = Field(  # noqa: UP007
        None,
        description='number of ray actor processes',
    )

    def get_executor(self) -> RayPoolExecutor:
        """Create an executor instance from the config."""
        return RayPoolExecutor(
            processes=self.ray_processes,
            ray_address=self.ray_address,
        )
