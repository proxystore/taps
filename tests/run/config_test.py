from __future__ import annotations

import pathlib
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    import tomllib
else:  # pragma: <3.11 cover
    import tomli as tomllib

from taps.run.config import Config
from testing.app import TestAppConfig


def test_read_write_toml_config(tmp_path: pathlib.Path) -> None:
    config = Config(app=TestAppConfig())

    config_file = tmp_path / 'config.toml'
    config.write_toml(config_file)

    with open(config_file) as f:
        config_data = tomllib.load(f)

    assert Config(**config_data) == config
