from __future__ import annotations

import sys
from unittest import mock

from taps.apps.configs.physics import PhysicsConfig


def test_create_app() -> None:
    config = PhysicsConfig(simulations=1)

    with mock.patch.dict(
        sys.modules,
        {'taps.apps.physics': mock.MagicMock()},
    ):
        config.get_app()
