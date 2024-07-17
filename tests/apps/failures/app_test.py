from __future__ import annotations

import contextlib
import pathlib

import pytest

from taps.apps.failures.app import FailureInjectionApp
from taps.apps.failures.types import FailureType
from taps.apps.failures.types import ParentDependencyError
from taps.engine import Engine
from testing.app import MockAppConfig


@pytest.mark.parametrize(
    'failure_type',
    (FailureType.FAILURE, FailureType.DEPENDENCY),
)
def test_no_failures(
    failure_type: FailureType,
    app_engine: Engine,
    tmp_path: pathlib.Path,
) -> None:
    app = FailureInjectionApp(
        base_config=MockAppConfig(tasks=3),
        failure_rate=0,
        failure_type=failure_type,
    )

    with contextlib.closing(app):
        app.run(app_engine, tmp_path)


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
def test_failures(
    failure_type: FailureType,
    exc_type: type[Exception],
    exc_msg: str,
    app_engine: Engine,
    tmp_path: pathlib.Path,
) -> None:
    app = FailureInjectionApp(
        base_config=MockAppConfig(tasks=1),
        failure_rate=1,
        failure_type=failure_type,
    )

    with contextlib.closing(app):
        with pytest.raises(exc_type, match=exc_msg):
            app.run(app_engine, tmp_path)
