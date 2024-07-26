from __future__ import annotations

import pathlib
import sys
from unittest import mock

from taps.apps.configs.moldesign import MoldesignConfig


def test_create_app(tmp_path: pathlib.Path) -> None:
    config = MoldesignConfig(dataset=tmp_path)

    with mock.patch.dict(
        sys.modules,
        {'taps.apps.moldesign.app': mock.MagicMock()},
    ):
        config.get_app()
