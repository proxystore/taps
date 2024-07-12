from __future__ import annotations

import pathlib
from concurrent.futures import Executor

from taps.executor.parsl import ParslLocalConfig


def test_get_local_executor(tmp_path: pathlib.Path) -> None:
    run_dir = str(tmp_path / 'parsl')
    config = ParslLocalConfig(run_dir=run_dir)
    executor = config.get_executor()
    assert isinstance(executor, Executor)
