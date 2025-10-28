from __future__ import annotations

import pathlib
import sys
from datetime import datetime
from datetime import timedelta
from unittest import mock

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    import tomllib
else:  # pragma: <3.11 cover
    import tomli as tomllib

import pytest
from pydantic import ValidationError

from taps.engine import EngineConfig
from taps.executor import ThreadPoolConfig
from taps.run.config import Config
from taps.run.config import LoggingConfig
from taps.run.config import make_run_dir
from taps.run.config import RunConfig
from testing.app import MockAppConfig


def test_create_config_default_plugins() -> None:
    Config(app=MockAppConfig())


def test_create_config_manual_plugins() -> None:
    Config(
        app=MockAppConfig(),
        engine=EngineConfig(),
        logging=LoggingConfig(),
        run=RunConfig(),
    )


def test_config_forbids_extra_args() -> None:
    with pytest.raises(ValidationError, match='unknown_options'):
        Config(app=MockAppConfig(), unknown_options=True)  # type: ignore[call-arg]


def test_config_equality() -> None:
    config1 = Config(app=MockAppConfig(tasks=1))
    config2 = Config(app=MockAppConfig(tasks=1))
    config3 = Config(app=MockAppConfig(tasks=2))

    assert config1 == config2
    assert config1 != config3

    with pytest.raises(NotImplementedError):
        assert config1 == {}


def test_read_write_toml_config(tmp_path: pathlib.Path) -> None:
    config = Config(app=MockAppConfig())

    config_file = tmp_path / 'config.toml'
    config.write_toml(config_file)
    assert config_file.is_file()

    new_config = Config.from_toml(config_file)
    assert config == new_config


def test_write_toml_config_includes_defaults(tmp_path: pathlib.Path) -> None:
    config = Config(app=MockAppConfig())

    config_file = tmp_path / 'config.toml'
    config.write_toml(config_file)

    with open(config_file, 'rb') as f:
        config_dict = tomllib.load(f)

    # We did not define the engine config, but the default engine config
    # should be included in the written config file.
    engine_dict = config_dict['engine']
    assert 'executor' in engine_dict
    assert 'filter' not in engine_dict
    assert 'transformer' not in engine_dict


def test_make_run_dir(tmp_path: pathlib.Path) -> None:
    dir_format = str(tmp_path / '{name}__{executor}__{timestamp}')
    config = Config(
        app=MockAppConfig(),
        engine=EngineConfig(executor=ThreadPoolConfig()),
        run=RunConfig(dir_format=dir_format),
    )

    run_dir = make_run_dir(config)
    assert run_dir.is_dir()

    name, executor, timestamp_str = run_dir.name.split('__')
    assert name == config.app.name
    assert executor == config.engine.executor.name

    # Check timestamp is < 5 seconds from now
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d-%H-%M-%S')
    assert datetime.now() - timestamp < timedelta(seconds=5)


def test_read_parsl_htex_config(tmp_path: pathlib.Path) -> None:
    config_toml = tmp_path / 'config.toml'

    config_raw = f"""\
[app]
name = "mock-app"

[engine.executor]
name = "parsl-htex"
retries = 3
strategy = "none"
run_dir = "{tmp_path / 'parsl'!s}"

[engine.executor.htex]
worker_ports = 0
max_workers_per_node = 4
cores_per_worker = 2

[engine.executor.htex.provider]
kind = "PBSProProvider"
account = "test-account"
cpus_per_node = 8
queue = "debug"

[engine.executor.htex.provider.launcher]
kind = "MpiExecLauncher"
bind_cmd = "--cpu-bind"
overrides = "--depth=8 --ppn=4"

[engine.executor.htex.address]
kind = "address_by_interface"
ifname = "bond0"
"""

    with open(config_toml, 'w') as f:
        f.write(config_raw)

    config = Config.from_toml(config_toml)

    with mock.patch.object(
        config.engine.executor.htex,  # type: ignore[attr-defined]
        'get_executor',
    ):
        with config.engine.executor.get_executor():
            pass
