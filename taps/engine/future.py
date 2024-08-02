"""Utilities for type checking third-party future types."""

from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

ResultT = TypeVar('ResultT')


@runtime_checkable
class FutureProtocol(Protocol[ResultT]):
    """Future protocol.

    This [`Protocol`][typing.Protocol] is useful for type checking future
    types that do not inherit from [`Future`][concurrent.futures.Future]
    (such as Dask's [`Future`][distributed.Future]).

    This protocol does not require `running()` because Dask does not provide
    that method.
    """

    def add_done_callback(
        self,
        callback: Callable[[FutureProtocol[ResultT]], Any],
    ) -> None:
        """Add a done callback to the future."""
        ...

    def cancel(self) -> bool:
        """Attempt to cancel the task."""
        ...

    def cancelled(self) -> bool:
        """Check if the task was cancelled."""
        ...

    def done(self) -> bool:
        """Check if the task is done."""
        ...

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Get the exception raised by the task."""
        ...

    def result(self, timeout: float | None = None) -> ResultT:
        """Get the result of the task."""
        ...


def is_future(obj: Any) -> bool:
    """Check if an object is future-like."""
    return isinstance(obj, FutureProtocol)
