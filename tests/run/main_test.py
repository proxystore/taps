from __future__ import annotations

from unittest import mock

from webs.executor.python import ThreadPoolConfig
from webs.run.config import BenchmarkConfig
from webs.run.main import main
from webs.run.main import parse_args_to_config
from webs.run.main import run


@mock.patch('webs.run.main.parse_args_to_config')
@mock.patch('webs.run.main.init_logging')
def test_main(mock_parse, mock_logging) -> None:
    with mock.patch('webs.run.main.run'):
        assert main() == 0


@mock.patch('webs.run.main.parse_args_to_config')
@mock.patch('webs.run.main.init_logging')
def test_main_error(mock_parse, mock_logging) -> None:
    with mock.patch('webs.run.main.run', side_effect=RuntimeError):
        assert main() == 1


def test_run(test_benchmark_config: BenchmarkConfig) -> None:
    run(test_benchmark_config)


def test_parse_args_to_config() -> None:
    argv = [
        'test-workflow',
        '--executor',
        'thread-pool',
    ]
    config = parse_args_to_config(argv)

    assert config.name == 'test-workflow'
    assert isinstance(config.executor, ThreadPoolConfig)
