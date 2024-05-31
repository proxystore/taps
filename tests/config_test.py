from __future__ import annotations

import argparse
from unittest import mock

import pytest

from taps.config import Config


class _TestConfig(Config):
    x: int
    y: str = 'y'


def test_config_add_argument_group() -> None:
    config = _TestConfig(x=42, y='default')
    parser = argparse.ArgumentParser()
    _TestConfig.add_argument_group(parser)
    args = parser.parse_args(['--x', str(config.x), '--y', config.y])
    assert config == _TestConfig(**vars(args))


@pytest.mark.parametrize('required', (True, False))
def test_config_add_argument_group_required(required: bool) -> None:
    parser = argparse.ArgumentParser()
    _TestConfig.add_argument_group(parser, required=required)

    if required:
        # Suppress argparse error message
        with mock.patch('argparse.ArgumentParser._print_message'):
            with pytest.raises(SystemExit):
                parser.parse_args([])
    else:
        parser.parse_args([])
