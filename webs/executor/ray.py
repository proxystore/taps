from __future__ import annotations

import sys
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import cast
from typing import Optional
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

try:
    import ray

    RAY_IMPORT_ERROR = None
except ImportError as e:  # pragma: no cover
    RAY_IMPORT_ERROR = e

from pydantic import Field

from webs.executor.config import ExecutorConfig
from webs.executor.config import register

P = ParamSpec('P')
T = TypeVar('T')


class RayExecutor(Executor):
    """Ray execution engine.

    Args:
        address: Address to pass to `ray.init()`.
        num_cpus: Number of CPUs to use.
    """

    def __init__(
        self,
        address: str | None = 'local',
        num_cpus: int | None = None,
    ) -> None:
        if RAY_IMPORT_ERROR is not None:  # pragma: no cover
            raise RAY_IMPORT_ERROR

        ray.init(address=address, configure_logging=False, num_cpus=num_cpus)
        # Mapping of Python callables to Ray RemoteFunction types
        self._remote: dict[Any, Any] = {}

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
                arg.object_ref if isinstance(arg, Future) else arg  # type: ignore[attr-defined]
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

        if function in self._remote:
            remote = self._remote[function]
        else:
            # This wrapper is needed because Ray will raise a TypeError
            # on certain function type that fail the inspect.isfunction
            # and inspect.isclass checks in
            # https://github.com/ray-project/ray/blob/6a8997cd720e2a92c5dc2763becf39e180b8c96e/python/ray/_private/worker.py#L3018-L3037
            def _wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                return function(*args, **kwargs)  # pragma: no cover

            remote = ray.remote(_wrapper)
            self._remote[function] = remote

        object_ref = remote.remote(*args, **kwargs)

        return object_ref.future()

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the client."""
        ray.shutdown()


@register(name='ray')
class RayConfig(ExecutorConfig):
    """Ray configuration.

    Attributes:
        ray_address: Address of the Ray cluster to run on.
        processes: Number of actor processes to start in the pool. Defaults to
            the number of cores in the Ray cluster, or the number of cores
            on this machine.
    """

    ray_address: Optional[str] = Field(  # noqa: UP007
        'local',
        description='ray scheduler address (default spawns local cluster)',
    )
    ray_num_cpus: Optional[int] = Field(  # noqa: UP007,
        None,
        description='maximum number of CPUs that ray will use',
    )

    def get_executor(self) -> RayExecutor:
        """Create an executor instance from the config."""
        return RayExecutor(
            address=self.ray_address,
            num_cpus=self.ray_num_cpus,
        )
