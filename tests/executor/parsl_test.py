from __future__ import annotations

import pathlib
from concurrent.futures import Executor

from taps.executor.parsl import ParslConfig


def test_get_thread_config(tmp_path: pathlib.Path) -> None:
    run_dir = str(tmp_path / 'parsl')
    config = ParslConfig(use_threads=True, run_dir=run_dir)
    config.get_executor_config()


def test_get_process_config(tmp_path: pathlib.Path) -> None:
    run_dir = str(tmp_path / 'parsl')
    config = ParslConfig(use_threads=False, run_dir=run_dir)
    config.get_executor_config()


def test_get_executor(tmp_path: pathlib.Path) -> None:
    run_dir = str(tmp_path / 'parsl')
    config = ParslConfig(use_threads=True, run_dir=run_dir)
    executor = config.get_executor()
    assert isinstance(executor, Executor)
