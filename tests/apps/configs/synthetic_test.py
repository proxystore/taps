from __future__ import annotations

import pytest
from pydantic import ValidationError

from taps.apps.configs.synthetic import SyntheticConfig
from taps.apps.configs.synthetic import WorkflowStructure


def test_create_app() -> None:
    config = SyntheticConfig(
        structure=WorkflowStructure.SEQUENTIAL,
        task_count=3,
    )
    assert config.get_app()


def test_validate_rate() -> None:
    with pytest.raises(
        ValidationError,
        match=(
            "Option 'bag_max_running' must be specified when "
            "'bag' is specified."
        ),
    ):
        SyntheticConfig(structure='bag', task_count=3)
