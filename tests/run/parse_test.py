from __future__ import annotations

import pathlib

import pytest

from taps.executor.python import ThreadPoolConfig
from taps.run.parse import parse_args_to_config
from testing.app import MockAppConfig


def test_exit_on_no_args(capsys) -> None:
    with pytest.raises(SystemExit):
        parse_args_to_config([])

    captured = capsys.readouterr()
    assert '--help' in captured.out


def test_exit_on_only_help(capsys) -> None:
    with pytest.raises(SystemExit):
        parse_args_to_config(['--help'])

    captured = capsys.readouterr()
    assert '--help' in captured.out


def test_parse_args_missing_app(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match='App name option is required.'):
        parse_args_to_config(['--engine.executor', 'process-pool'])

    config_file = tmp_path / 'config.toml'
    with open(config_file, 'w') as f:
        f.write('[app]')

    with pytest.raises(ValueError, match='App name option is required.'):
        parse_args_to_config(['--config', str(config_file)])


def test_parse_cli_args_only() -> None:
    argv = [
        '--app',
        'mock-app',
        '--engine.executor',
        'thread-pool',
    ]
    config = parse_args_to_config(argv)

    assert isinstance(config.app, MockAppConfig)
    assert isinstance(config.engine.executor, ThreadPoolConfig)


def test_parse_config_file_only(tmp_path: pathlib.Path) -> None:
    config_text = """\
[app]
name = "mock-app"
tasks = 0
"""

    config_file = tmp_path / 'config.toml'
    with open(config_file, 'w') as f:
        f.write(config_text)

    config = parse_args_to_config(['--config', str(config_file)])

    assert isinstance(config.app, MockAppConfig)
    assert config.app.tasks == 0


def test_parse_multiple_config_file(tmp_path: pathlib.Path) -> None:
    config_text_1 = """\
[app]
name = "mock-app"
tasks = 0
"""
    config_text_2 = """\
[app]
tasks = 1
"""

    config_file_1 = tmp_path / 'config-1.toml'
    with open(config_file_1, 'w') as f:
        f.write(config_text_1)

    config_file_2 = tmp_path / 'config-2.toml'
    with open(config_file_2, 'w') as f:
        f.write(config_text_2)

    config = parse_args_to_config(
        ['--config', str(config_file_1), str(config_file_2)],
    )

    assert isinstance(config.app, MockAppConfig)
    # config 2 should override config 1
    assert config.app.tasks == 1


def test_parse_cli_args_and_config_file(tmp_path: pathlib.Path) -> None:
    config_text = """\
[app]
name = "mock-app"
tasks = 0
"""

    config_file = tmp_path / 'config.toml'
    with open(config_file, 'w') as f:
        f.write(config_text)

    args = ['--config', str(config_file), '--app.tasks', '1']
    config = parse_args_to_config(args)

    assert isinstance(config.app, MockAppConfig)
    # CLI arg should take precedence over config file
    assert config.app.tasks == 1
