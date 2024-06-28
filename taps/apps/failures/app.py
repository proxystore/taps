from __future__ import annotations

import functools
import logging
import pathlib
import random
import sys
from typing import Any
from typing import Callable
from typing import cast
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from taps.apps import AppConfig
from taps.apps.failures.types import FAILURE_FUNCTIONS
from taps.apps.failures.types import FailureType
from taps.engine import TaskFuture
from taps.engine.engine import Engine
from taps.logging import APP_LOG_LEVEL

P = ParamSpec('P')
T = TypeVar('T')

logger = logging.getLogger(__name__)


class _FailureInjectionEngine(Engine):
    def __init__(
        self,
        engine: Engine,
        failure_rate: float,
        failure_type: FailureType,
    ) -> None:
        self.engine = engine
        self.failure_rate = failure_rate
        self.failure_type = failure_type

        self._failure_tasks: dict[
            tuple[FailureType, Callable[[Any], Any]],
            Callable[[Any], Any],
        ] = {}

    def create_failure_task(
        self,
        task: Callable[P, T],
    ) -> Callable[P, T]:
        failure_type = self.failure_type
        if failure_type == FailureType.RANDOM:
            options = [f.value for f in FailureType]
            options.remove(FailureType.RANDOM.value)
            failure_type = FailureType(random.choice(options))

        failure_task = self._failure_tasks.get((failure_type, task), None)
        if failure_task is not None:
            return cast(Callable[P, T], failure_task)

        failure_function = FAILURE_FUNCTIONS[failure_type]

        @functools.wraps(task)
        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            failure_function()
            # Typically the above failure should prevent the task
            # from actually executing.
            return task(*args, **kwargs)

        self._failure_tasks[(failure_type, task)] = _wrapped
        return _wrapped

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> TaskFuture[T]:
        if random.random() > self.failure_rate:
            return self.engine.submit(function, *args, **kwargs)

        if self.failure_type == FailureType.DEPENDENCY:
            # Submit a parent task that will raise an exception.
            parent_task = self.engine.submit(
                FAILURE_FUNCTIONS[FailureType.FAILURE],
            )
            # Pass the future of the parent to the actual task. The
            # underlying executor will wait on the parent_task future, see
            # that it error, and then act appropriately.
            return self.engine.submit(function, parent_task, *args, **kwargs)
        else:
            fail_function = self.create_failure_task(function)
            return self.engine.submit(fail_function, *args, **kwargs)

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        self.engine.shutdown(wait=wait, cancel_futures=cancel_futures)


class FailureInjectionApp:
    """Failure injection application.

    Args:
        base_config: Configuration for the base application to inject failures
            into.
        failure_type: The type of failure to inject.
        failure_rate: The probability of injecting a failure into any
            given task.
    """

    def __init__(
        self,
        base_config: AppConfig,
        failure_rate: float,
        failure_type: FailureType,
    ) -> None:
        self.base = base_config.get_app()
        self.base_config = base_config
        self.failure_rate = failure_rate
        self.failure_type = failure_type

    def close(self) -> None:
        """Close the application."""
        self.base.close()

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        logger.log(
            APP_LOG_LEVEL,
            f'Injecting failures into the {self.base_config.name} app '
            f'(type={self.failure_type.value}, rate={self.failure_rate})',
        )

        with _FailureInjectionEngine(
            engine,
            failure_rate=self.failure_rate,
            failure_type=self.failure_type,
        ) as failure_engine:
            self.base.run(engine=failure_engine, run_dir=run_dir)
