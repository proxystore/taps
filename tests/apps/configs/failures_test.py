from __future__ import annotations

import re

import pytest
from pydantic import ValidationError

from taps.apps.configs.failures import FailureInjectionConfig


def test_create_app() -> None:
    config = FailureInjectionConfig(
        base='mock-app',
        config={'tasks': 3},
        failure_rate=0.5,
        failure_type='import',
    )
    assert config.get_app()


def test_validate_base_app() -> None:
    with pytest.raises(
        ValidationError,
        match='Base app named "fake-app" is unknown.',
    ):
        FailureInjectionConfig(base='fake-app')


def test_validate_rate() -> None:
    with pytest.raises(
        ValidationError,
        match=re.escape('Failure rate must be in the range [0, 1]. Got 2.1.'),
    ):
        FailureInjectionConfig(base='mock-app', failure_rate=2.1)
