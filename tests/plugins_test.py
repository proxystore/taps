from __future__ import annotations

import pytest
from pydantic import BaseModel

from taps.plugins import get_app_configs
from taps.plugins import get_executor_configs
from taps.plugins import get_filter_configs
from taps.plugins import get_transformer_configs
from taps.plugins import register


def test_get_app_configs() -> None:
    assert len(get_app_configs()) > 1


def test_get_executor_configs() -> None:
    assert len(get_executor_configs()) > 1


def test_get_filter_configs() -> None:
    assert len(get_filter_configs()) > 1


def test_get_transformer_configs() -> None:
    assert len(get_transformer_configs()) > 1


def test_register_bad_kind() -> None:
    with pytest.raises(ValueError, match='Unknown plugin type "test".'):
        register('test')(BaseModel)  # type: ignore[arg-type]


def test_register_config_missing_name() -> None:
    with pytest.raises(
        RuntimeError,
        match='Failed to register BaseModel as a app plugin',
    ):
        register('app')(BaseModel)
