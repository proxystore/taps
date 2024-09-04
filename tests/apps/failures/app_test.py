from __future__ import annotations

import contextlib
import pathlib
from typing import Callable

import pytest

from taps.apps.failures.app import _FailureInjectionEngine
from taps.apps.failures.app import FailureInjectionApp
from taps.apps.failures.types import FailureType
from taps.apps.failures.types import ParentDependencyError
from taps.engine import Engine
from taps.engine.task import Task
from testing.app import MockAppConfig


@pytest.mark.parametrize(
    'failure_type',
    (FailureType.FAILURE, FailureType.DEPENDENCY),
)
def test_app_without_failures(
    failure_type: FailureType,
    engine: Engine,
    tmp_path: pathlib.Path,
) -> None:
    app = FailureInjectionApp(
        base_config=MockAppConfig(tasks=3),
        failure_rate=0,
        failure_type=failure_type,
    )

    with contextlib.closing(app):
        app.run(engine, tmp_path)


@pytest.mark.parametrize(
    ('failure_type', 'exc_type', 'exc_msg'),
    (
        (FailureType.FAILURE, Exception, 'Failure injection error.'),
        (
            FailureType.DEPENDENCY,
            ParentDependencyError,
            'Simulated failure in parent task.',
        ),
    ),
)
def test_app_with_failures(
    failure_type: FailureType,
    exc_type: type[Exception],
    exc_msg: str,
    engine: Engine,
    tmp_path: pathlib.Path,
) -> None:
    app = FailureInjectionApp(
        base_config=MockAppConfig(tasks=1),
        failure_rate=1,
        failure_type=failure_type,
    )

    with contextlib.closing(app):
        with pytest.raises(exc_type, match=exc_msg):
            app.run(engine, tmp_path)


@pytest.mark.parametrize('as_task', (True, False))
def test_failure_injection_engine(as_task: bool, engine: Engine) -> None:
    failure_engine = _FailureInjectionEngine(
        engine,
        failure_rate=0,
        failure_type=FailureType.DEPENDENCY,
    )

    def my_abs(x: int) -> int:
        return abs(x)

    function: Callable[[int], int] = (
        Task(my_abs) if as_task else my_abs  # type: ignore[assignment]
    )
    future = failure_engine.submit(function, -1)
    assert future.result() == 1
