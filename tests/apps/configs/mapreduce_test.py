from __future__ import annotations

import pathlib

from taps.apps.configs.mapreduce import MapreduceConfig


def test_mapreduce_config(tmp_path: pathlib.Path) -> None:
    config = MapreduceConfig(data_dir=tmp_path)
    config.get_app()
