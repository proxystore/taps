from __future__ import annotations

import contextlib
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Generator
from typing import TypeVar
from unittest import mock

import globus_compute_sdk

RT = TypeVar('RT')


class MockGlobusComputeExecutor(globus_compute_sdk.Executor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def submit(
        self,
        func: Callable[..., RT],
        *args: Any,
        **kwargs: Any,
    ) -> Future[RT]:
        fut: Future[Any] = Future()
        fut.set_result(func(*args, **kwargs))
        return fut

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        pass


@contextlib.contextmanager
def mock_globus_compute() -> Generator[None, None, None]:
    with mock.patch(
        'globus_compute_sdk.Client',
    ), mock.patch('globus_compute_sdk.Executor', MockGlobusComputeExecutor):
        yield
