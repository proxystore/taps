from __future__ import annotations

import pathlib

import taps
from taps.run.env import _get_version
from taps.run.env import Environment


def test_collect_environment() -> None:
    env = Environment.collect()
    assert isinstance(env.format(), str)


def test_write_environment_json(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'env.json'
    env = Environment.collect()
    env.write_json(filepath)
    assert filepath.is_file()


def test_get_version() -> None:
    assert _get_version('taps') == taps.__version__
    assert _get_version('foo-package') == 'not installed'
    assert _get_version('taps.run') == 'not found'
