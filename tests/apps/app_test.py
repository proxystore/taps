from __future__ import annotations

import pathlib

from taps.apps.app import App
from taps.apps.app import AppConfig


def test_parse_config_paths() -> None:
    class TestConfig(AppConfig):
        name: str
        path: pathlib.Path

        def get_app(self) -> App:
            raise NotImplementedError

    # AppConfig should parse str to pathlib.Path then force
    # paths to be absolute.
    config = TestConfig(name='test', path='test-parse-config-paths')
    assert config.path.is_absolute()

    # AppConfig should serialize all pathlib.Path types as
    # strings.
    dump = config.model_dump()
    assert isinstance(dump['path'], str)
