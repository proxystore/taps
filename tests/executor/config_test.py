from __future__ import annotations

import argparse
from concurrent.futures import Executor
from unittest import mock

import pytest

from webs.executor.config import ExecutorChoicesConfig
from webs.executor.config import ExecutorConfig
from webs.executor.config import get_executor_config
from webs.executor.config import register


@register(name='test-executor')
class _TestConfig(ExecutorConfig):
    x: int
    y: str

    def get_executor(self) -> Executor:
        raise NotImplementedError


def test_executor_choices_argument_group():
    config = _TestConfig(x=42, y='default')
    argv = [
        '--executor',
        'test-executor',
        '--x',
        str(config.x),
        '--y',
        config.y,
    ]

    parser = argparse.ArgumentParser()
    ExecutorChoicesConfig.add_argument_group(parser)

    args = parser.parse_args(argv)

    assert get_executor_config(**vars(args)) == config


def test_executor_choices_argument_group_required():
    argv = ['--executor', 'test-executor']

    parser = argparse.ArgumentParser()
    ExecutorChoicesConfig.add_argument_group(parser, argv=argv, required=True)

    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args(argv)
