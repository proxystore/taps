from __future__ import annotations

import sys
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Literal
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

from taps.executor import ExecutorConfig
from taps.plugins import register

P = ParamSpec('P')
T = TypeVar('T')


def _parse_arg(arg: Any) -> Any:
    if isinstance(arg, Future):
        return arg.object_ref  # type: ignore[attr-defined]  # pragma: no cover
    else:
        return arg


def _parse_args(args: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(map(_parse_arg, args))


def _parse_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {k: _parse_arg(v) for k, v in kwargs.items()}


# This wrapper is needed because Ray will raise a TypeError
# on certain function type that fail the inspect.isfunction
# and inspect.isclass checks in
# https://github.com/ray-project/ray/blob/6a8997cd720e2a92c5dc2763becf39e180b8c96e/python/ray/_private/worker.py#L3018-L3037
def _wrap_function(function: Callable[P, T]) -> Callable[P, T]:
    def _wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        return function(*args, **kwargs)  # pragma: no cover

    return _wrapper


class RayExecutor(Executor):
    """Ray execution engine.

    Note:
        Ray will raise a serialization error if a
        [`Proxy[bytes]`][proxystore.proxy.Proxy] is passed to or returned
        by a function. This is because Ray skips serializing [`bytes`][bytes]
        instances. Ray works with all other types of proxies, so if you need
        to send [`bytes`][bytes] data, wrap the data in another type.

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

        self._address = address
        self._num_cpus = num_cpus

        ray.init(address=address, configure_logging=False, num_cpus=num_cpus)
        # Mapping of Python callables to Ray RemoteFunction types
        self._remote: dict[Any, Any] = {}

    def __repr__(self) -> str:
        address = f"'{self._address}'" if self._address is not None else None
        return (
            f'{type(self).__name__}'
            f'(address={address}, num_cpus={self._num_cpus})'
        )

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
        pargs = _parse_args(args)
        pkwargs = _parse_kwargs(kwargs)

        if function in self._remote:
            remote = self._remote[function]
        else:
            wrapped = _wrap_function(function)
            remote = ray.remote(wrapped)
            self._remote[function] = remote

        object_ref = remote.remote(*pargs, **pkwargs)

        return object_ref.future()

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the client."""
        ray.shutdown()


@register('executor')
class RayConfig(ExecutorConfig):
    """[`RayExecutor`][taps.executor.ray.RayExecutor] plugin configuration."""

    name: Literal['ray'] = Field('ray', description='Executor name.')
    address: Optional[str] = Field(  # noqa: UP007
        'local',
        description='Ray scheduler address (default spawns local cluster).',
    )
    num_cpus: Optional[int] = Field(  # noqa: UP007,
        None,
        description='Maximum number of CPUs that ray will use.',
    )

    def get_executor(self) -> RayExecutor:
        """Create an executor instance from the config."""
        return RayExecutor(address=self.address, num_cpus=self.num_cpus)
