from __future__ import annotations

import pathlib
import socket
import sys
from concurrent.futures import Executor
from unittest import mock

import pytest
from parsl.addresses import address_by_interface
from parsl.executors import HighThroughputExecutor
from parsl.launchers.base import Launcher
from parsl.providers.base import ExecutionProvider
from pydantic import ValidationError

from taps.executor.parsl import AddressConfig
from taps.executor.parsl import HTExConfig
from taps.executor.parsl import LauncherConfig
from taps.executor.parsl import ParslHTExConfig
from taps.executor.parsl import ParslLocalConfig
from taps.executor.parsl import ProviderConfig


def test_get_local_executor(tmp_path: pathlib.Path) -> None:
    run_dir = str(tmp_path / 'parsl')
    config = ParslLocalConfig(run_dir=run_dir)
    executor = config.get_executor()
    assert isinstance(executor, Executor)


def test_get_htex_executor(tmp_path: pathlib.Path) -> None:
    htex_config = HTExConfig(
        provider=ProviderConfig(
            kind='PBSProProvider',
            launcher=LauncherConfig(kind='SimpleLauncher'),
            account='test-account',
            cpus_per_node=32,
            queue='debug',
        ),
        address=AddressConfig(kind='address_by_hostname'),
        worker_ports=[0, 0],
        worker_port_range=[0, 0],
        interchange_port_range=[0, 0],
    )

    config = ParslHTExConfig(
        htex=htex_config,
        app_cache=False,
        retries=1,
        run_dir=str(tmp_path / 'parsl'),
        max_workers_per_node=4,
    )

    with mock.patch.object(htex_config, 'get_executor'):
        executor = config.get_executor()
    assert isinstance(executor, Executor)


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='address resolution is unreliable on MacOS',
)
def test_address_config() -> None:
    ifname = socket.if_nameindex()[0][1]
    config = AddressConfig(kind='address_by_interface', ifname=ifname)
    assert config.get_address() == address_by_interface(ifname=ifname)


def test_address_config_unknown_kind() -> None:
    with pytest.raises(ValidationError, match='fake_address'):
        AddressConfig(kind='fake_address')


def test_launcher_config() -> None:
    config = LauncherConfig(
        kind='MpiExecLauncher',
        bind_cmd='--cpu-bind',
        overrides='--depth=64 --ppn=1',
    )
    assert isinstance(config.get_launcher(), Launcher)


def test_launcher_config_unknown_kind() -> None:
    with pytest.raises(ValidationError, match='FakeLauncher'):
        LauncherConfig(kind='FakeLauncher')


def test_provider_config() -> None:
    config = ProviderConfig(
        kind='PBSProProvider',
        launcher=LauncherConfig(kind='SimpleLauncher'),
        account='test-account',
        cpus_per_node=32,
        queue='debug',
    )
    assert isinstance(config.get_provider(), ExecutionProvider)


def test_provider_config_default() -> None:
    config = ProviderConfig(kind='AdHocProvider')
    assert isinstance(config.get_provider(), ExecutionProvider)


def test_provider_config_unknown_kind() -> None:
    with pytest.raises(ValidationError, match='FakeProvider'):
        ProviderConfig(kind='FakeProvider')


def test_htex_config() -> None:
    config = HTExConfig(
        provider=ProviderConfig(
            kind='PBSProProvider',
            launcher=LauncherConfig(kind='SimpleLauncher'),
            account='test-account',
            cpus_per_node=32,
            queue='debug',
        ),
        address=AddressConfig(kind='address_by_hostname'),
        worker_ports=[0, 0],
        worker_port_range=[0, 0],
        interchange_port_range=[0, 0],
    )
    assert isinstance(config.get_executor(), HighThroughputExecutor)


def test_htex_config_default() -> None:
    config = HTExConfig()
    assert isinstance(config.get_executor(), HighThroughputExecutor)
