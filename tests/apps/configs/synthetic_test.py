from __future__ import annotations

import pytest
from pydantic import ValidationError

from taps.apps.configs.synthetic import SyntheticConfig


def test_create_app() -> None:
    config = SyntheticConfig(structure='sequential', task_count=3)
    assert config.get_app()


def test_validate_base_app() -> None:
    with pytest.raises(
        ValidationError,
        match="Specified structure 'fake' is unknown.",
    ):
        SyntheticConfig(structure='fake', task_count=3)


def test_validate_rate() -> None:
    with pytest.raises(
        ValidationError,
        match=(
            "Option 'bag_max_running' must be specified when "
            "'bag' is specified."
        ),
    ):
        SyntheticConfig(structure='bag', task_count=3)
