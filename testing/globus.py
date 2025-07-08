from __future__ import annotations

import contextlib
import sys
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Generator
from typing import TypeVar
from unittest import mock

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

import globus_compute_sdk

P = ParamSpec('P')
T = TypeVar('T')


class MockGlobusComputeExecutor(globus_compute_sdk.Executor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def submit(  # type: ignore[override]
        self,
        func: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        fut: Future[Any] = Future()
        fut.set_result(func(*args, **kwargs))
        return fut

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        pass


@contextlib.contextmanager
def mock_globus_compute() -> Generator[None, None, None]:
    with (
        mock.patch(
            'globus_compute_sdk.Client',
        ),
        mock.patch('globus_compute_sdk.Executor', MockGlobusComputeExecutor),
    ):
        yield
