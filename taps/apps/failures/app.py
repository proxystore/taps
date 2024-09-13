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
from taps.apps.failures.types import ParentDependencyError
from taps.engine import Engine
from taps.engine import task
from taps.engine import TaskFuture
from taps.engine.task import Task
from taps.engine.task import TaskResult
from taps.logging import APP_LOG_LEVEL

P = ParamSpec('P')
R = TypeVar('R')

logger = logging.getLogger(__name__)


@task(name='injected_parent_task')
def _dependency_failure_parent_task(
    failure_rate: float,
) -> None:
    if random.random() <= failure_rate:
        raise ParentDependencyError('Simulated failure in parent task.')


def _wrapper_dependency_failure(
    task: Task[P, R],
    parent: Any,
    *args: P.args,
    **kwargs: P.kwargs,
) -> TaskResult[R]:
    assert '_transformer' in kwargs
    return task(*args, **kwargs)  # type: ignore[arg-type,return-value]


def _wrapper_generic_failure(
    task: Task[P, R],
    failure_function: Callable[[], None],
    failure_rate: float,
    *args: P.args,
    **kwargs: P.kwargs,
) -> TaskResult[R]:
    if random.random() <= failure_rate:
        # This will error and not return
        failure_function()

    assert '_transformer' in kwargs
    return task(*args, **kwargs)  # type: ignore[arg-type,return-value]


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
            Task[Any, Any],
        ] = {}

    def create_failure_task(
        self,
        function: Callable[P, R],
    ) -> tuple[Task[P, R], FailureType]:
        task = self.engine._get_task(function)

        failure_type = (
            FailureType.random()
            if self.failure_type is FailureType.RANDOM
            else self.failure_type
        )

        failure_task = self._failure_tasks.get((failure_type, function), None)
        if failure_task is not None:
            return cast(Task[P, R], failure_task), failure_type

        if failure_type == FailureType.DEPENDENCY:
            wrapped = functools.partial(_wrapper_dependency_failure, task)
        else:
            wrapped = functools.partial(
                _wrapper_generic_failure,
                task,
                FAILURE_FUNCTIONS[failure_type],
                self.failure_rate,
            )

        wrapped.__dict__['name'] = task.name
        wrapped.__dict__['__wrapped__'] = task
        assert isinstance(wrapped, Task)

        self._failure_tasks[(failure_type, function)] = wrapped
        return wrapped, failure_type

    def submit(
        self,
        function: Callable[P, R],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> TaskFuture[R]:
        wrapped, failure_type = self.create_failure_task(function)

        if failure_type == FailureType.DEPENDENCY:
            # Submit a parent task that will raise an exception.
            parent = self.engine.submit(
                _dependency_failure_parent_task,
                self.failure_rate,
            )
            # Pass the future of the parent to the actual task. The
            # underlying executor will wait on the parent_task future, see
            # that it error, and then act appropriately.
            return self.engine.submit(wrapped, parent, *args, **kwargs)
        else:
            return self.engine.submit(wrapped, *args, **kwargs)

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        # Do not close self.engine here because that is the responsibility
        # of the caller of FailureInjectionApp.run().
        pass


class FailureInjectionApp:
    """Failure injection application.

    Warning:
        This app will intercept the tasks submitted by the base application
        and modify the wrapped functions with the injected errors. Thus,
        failure injection may cause incompatibilities with executors that
        cannot serialize tasks by value (i.e., those that only serialize
        submitted functions by reference).

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
