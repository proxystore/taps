from __future__ import annotations

import sys
from unittest import mock

from taps.apps.configs.fedlearn import FedlearnConfig


def test_create_app() -> None:
    with mock.patch.dict(
        sys.modules,
        {'taps.apps.fedlearn.types': mock.MagicMock()},
    ):
        config = FedlearnConfig()

        with mock.patch.dict(
            sys.modules,
            {'taps.apps.fedlearn.app': mock.MagicMock()},
        ):
            config.get_app()
