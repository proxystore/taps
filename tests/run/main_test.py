from __future__ import annotations

import logging
import pathlib
from unittest import mock

from taps.run.config import Config
from taps.run.config import RunConfig
from taps.run.main import main
from taps.run.main import run
from testing.app import MockAppConfig


@mock.patch('taps.run.main.parse_args_to_config')
@mock.patch('taps.run.main.init_logging')
def test_main(mock_logging, mock_parse, tmp_path: pathlib.Path) -> None:
    mock_parse.return_value = Config(
        app=MockAppConfig(),
        run=RunConfig(dir_format=str(tmp_path)),
    )
    with mock.patch('taps.run.main.run'):
        assert main() == 0


@mock.patch('taps.run.main.parse_args_to_config')
@mock.patch('taps.run.main.init_logging')
def test_main_error(mock_logging, mock_parse, tmp_path: pathlib.Path) -> None:
    mock_parse.return_value = Config(
        app=MockAppConfig(),
        run=RunConfig(dir_format=str(tmp_path)),
    )
    with mock.patch('taps.run.main.run', side_effect=RuntimeError):
        assert main() == 1


def test_run(test_benchmark_config: Config, tmp_path: pathlib.Path) -> None:
    run(test_benchmark_config, tmp_path)


def test_run_log_version_mismatch(
    test_benchmark_config: Config,
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    test_benchmark_config.version = '0.0.0'
    with caplog.at_level(logging.WARNING, logger='taps.run'):
        run(test_benchmark_config, tmp_path)

    messages = [
        message
        for message in caplog.messages
        if 'The configuration specifies TaPS version 0.0.0' in message
    ]
    assert len(messages) == 1
