from __future__ import annotations

import pathlib
from unittest import mock

from taps.executor.python import ThreadPoolConfig
from taps.run.config import Config
from taps.run.main import main
from taps.run.main import parse_args_to_config
from taps.run.main import run


@mock.patch('taps.run.main.parse_args_to_config')
@mock.patch('taps.run.main.init_logging')
def test_main(mock_parse, mock_logging) -> None:
    with mock.patch('taps.run.main.run'):
        assert main() == 0


@mock.patch('taps.run.main.parse_args_to_config')
@mock.patch('taps.run.main.init_logging')
def test_main_error(mock_parse, mock_logging) -> None:
    with mock.patch('taps.run.main.run', side_effect=RuntimeError):
        assert main() == 1


def test_run(test_benchmark_config: Config, tmp_path: pathlib.Path) -> None:
    run(test_benchmark_config, tmp_path)


def test_parse_args_to_config(test_benchmark_config: Config) -> None:
    argv = [
        test_benchmark_config.name,
        '--executor',
        'thread-pool',
    ]
    config = parse_args_to_config(argv)

    assert config.name == test_benchmark_config.name
    assert isinstance(config.executor, ThreadPoolConfig)
