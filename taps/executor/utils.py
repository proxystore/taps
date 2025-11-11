from __future__ import annotations

import functools
import itertools
import logging
import socket
import sys
import threading
import time
from concurrent.futures import Executor
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generator
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import Mapping
from typing import ParamSpec
from typing import Sequence
from typing import TypeVar

from taps.future import FutureProtocol
from taps.logging import get_repr

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

logger = logging.getLogger(__name__)

P = ParamSpec('P')
T = TypeVar('T')


def _get_chunks(
    *iterables: Iterable[T],
    chunksize: int,
) -> Generator[tuple[tuple[T, ...], ...], None, None]:
    it = zip(*iterables, strict=False)
    while True:
        chunk = tuple(itertools.islice(it, chunksize))
        if not chunk:
            return
        yield chunk


def _process_chunk(
    function: Callable[..., T],
    chunk: Iterable[Any],
) -> list[T]:
    return [function(*args) for args in chunk]


class _Task(Generic[T]):
    def __init__(
        self,
        executor: Executor,
        function: Callable[..., T],
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
        client_future: Future[T],
    ) -> None:
        self.executor = executor
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.client_future = client_future
        self.task_future: FutureProtocol[T] | None = None
        self._pending_futures: set[FutureProtocol[Any]] = set()
        self._submit_lock = threading.RLock()

        for arg in [*args, *kwargs.values()]:
            if isinstance(arg, FutureProtocol):
                self._pending_futures.add(arg)

        # The callbacks added here mutate self.pending_futures so
        # we can't iterate on it directly.
        for future in tuple(self._pending_futures):
            future.add_done_callback(self._pending_future_callback)

        if len(self._pending_futures) == 0 and self.task_future is None:
            self._submit()

    def _pending_future_callback(self, future: FutureProtocol[Any]) -> None:
        if future in self._pending_futures:
            self._pending_futures.remove(future)
        else:  # pragma: no cover
            # This callback for this future has already been invoked so
            # do nothing. This shouldn't happen, but we can't assume
            # every executor will uphold the "callbacks are invoked once"
            # promise.
            return

        if future.cancelled():
            self.client_future.cancel()
        elif future.exception() is not None:
            self.client_future.set_exception(future.exception())
        elif len(self._pending_futures) == 0:
            self._submit()

    def _task_future_callback(self, future: FutureProtocol[T]) -> None:
        if future.cancelled():
            self.client_future.cancel()
        elif future.exception() is not None:
            self.client_future.set_exception(future.exception())
        else:
            self.client_future.set_result(future.result())

    def _submit(self) -> None:
        with self._submit_lock:
            if self.task_future is not None:  # pragma: no cover
                # Another thread already called _submit().
                return

            if self.client_future.cancelled():
                # client_future was cancelled so don't submit the task.
                return

            args = tuple(
                arg.result() if isinstance(arg, FutureProtocol) else arg
                for arg in self.args
            )
            kwargs = {
                key: value.result()
                if isinstance(value, FutureProtocol)
                else value
                for key, value in self.kwargs.items()
            }

            self.task_future = self.executor.submit(
                self.function,
                *args,
                **kwargs,
            )
            self.task_future.add_done_callback(self._task_future_callback)


class FutureDependencyExecutor(Executor):
    """Executor wrapper that adds DAG-like features.

    An [`Executor`][concurrent.futures.Executor] implementation that wraps
    another executor with logic for delaying task submission until all
    [`Future`][concurrent.futures.Future] instances which are args or kwargs
    of a task have completed. In other words, child tasks will not be
    scheduled until the results of the child's parent tasks are available.

    Args:
        executor: Executor to wrap.
    """

    def __init__(self, executor: Executor) -> None:
        self.executor = executor
        self._tasks: dict[Future[Any], _Task[Any]] = {}

    def __enter__(self) -> Self:
        self.executor.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> bool | None:
        return self.executor.__exit__(exc_type, exc_value, exc_traceback)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(executor={get_repr(self.executor)})'

    def _task_future_callback(self, future: Future[Any]) -> None:
        self._tasks.pop(future)

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
            [`Future`][concurrent.futures.Future] object representing the \
            result of the execution of the callable.
        """
        client_future: Future[T] = Future()
        task = _Task(self.executor, function, args, kwargs, client_future)
        self._tasks[client_future] = task
        client_future.add_done_callback(self._task_future_callback)
        return client_future

    def map(
        self,
        function: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
        buffersize: int | None = None,
    ) -> Iterator[T]:
        """Map a function onto iterables of arguments.

        Args:
            function: A callable that will take as many arguments as there are
                passed iterables.
            iterables: Variable number of iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: If greater than one, the iterables will be chopped into
                chunks of size chunksize and submitted to the executor. If set
                to one, the items in the list will be sent one at a time.
            buffersize: Ignored.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.

        Raises:
            ValueError: if chunksize is less than one.
        """
        # Based on concurrent.futures.ProcessPoolExecutor.map()
        # https://github.com/python/cpython/blob/37959e25cbbe1d207c660b5bc9583b9bd1403f1a/Lib/concurrent/futures/process.py
        if chunksize < 1:
            raise ValueError('chunksize must be >= 1.')

        results = super().map(
            functools.partial(_process_chunk, function),
            _get_chunks(*iterables, chunksize=chunksize),
            timeout=timeout,
        )

        def _result_iterator(
            iterable: Iterator[list[T]],
        ) -> Generator[T, None, None]:
            for element in iterable:
                element.reverse()
                while element:
                    yield element.pop()

        return _result_iterator(results)

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the executor.

        Args:
            wait: Wait on all pending futures to complete.
            cancel_futures: Cancel all pending futures that the executor
                has not started running.
        """
        self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)


def _warmup_task() -> str:
    # Used internally by warmup_executor()
    return socket.gethostname()


def warmup_executor(
    executor: Executor,
    min_connected_nodes: int,
    batch_size: int,
    max_batches: int,
    batch_sleep: int,
) -> None:
    """Warm up an executor until enough nodes are seen.

    Submits a bag of tasks to the executor where each task returns the
    hostname the task was executed on. Unique hostnames are used to identify
    active nodes.

    Args:
        executor: Executor to warm up.
        min_connected_nodes: Number of unique nodes necessary to consider
            the executor warm.
        batch_size: Number of tasks to submit.
        max_batches: Number of iterations to try to warm nodes.
        batch_sleep: Seconds to sleep between batches.

    Raises:
        RuntimeError: If `min_connected_nodes` nodes are not detected
            within `max_batches` warmup iterations.
    """
    hosts = set()
    for i in range(max_batches):
        futures = [executor.submit(_warmup_task) for _ in range(batch_size)]
        logger.info(
            f'Submitting warmup batch of tasks (batch={i + 1}/{max_batches}, '
            f'tasks={batch_size})',
        )

        for f in futures:
            hosts.add(f.result())

        if len(hosts) >= min_connected_nodes:
            logger.info(f'Executor connected to {len(hosts)} node(s)')
            logger.debug(f'Connected hosts: {hosts}')
            return

        time.sleep(batch_sleep)

    raise RuntimeError(
        f'Could not connect to {min_connected_nodes} nodes within '
        f'{max_batches} warmup batches of {batch_size} tasks.'
        f'Found {len(hosts)} unique nodes.',
    )
