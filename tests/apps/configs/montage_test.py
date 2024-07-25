from __future__ import annotations

import pathlib

from taps.apps.configs.montage import MontageConfig


def test_get_app(tmp_path: pathlib.Path) -> None:
    config = MontageConfig(img_folder=tmp_path)
    app = config.get_app()
    app.close()
