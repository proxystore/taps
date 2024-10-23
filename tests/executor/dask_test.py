from __future__ import annotations

from unittest import mock

import pytest
from dask.distributed import Client

from taps.executor.dask import DaskDistributedConfig
from taps.executor.dask import DaskDistributedExecutor


@pytest.fixture
def local_client() -> Client:
    client = Client(
        n_workers=1,
        processes=False,
        # Ideally we would disable the dashboard, but disabling is bugged
        # so set a random port to prevent issues.
        # See: https://github.com/dask/distributed/issues/8136
        dashboard_address=':0',
        worker_dashboard_address=':0',
    )
    return client


def test_submit_function(local_client: Client) -> None:
    with DaskDistributedExecutor(local_client) as executor:
        assert isinstance(repr(executor), str)

        future = executor.submit(round, 1.75, ndigits=1)
        expected = 1.8
        assert future.result() == expected


def test_map_function(local_client: Client) -> None:
    def _sum(x: int, y: int) -> int:
        return x + y

    with DaskDistributedExecutor(local_client) as executor:
        assert isinstance(repr(executor), str)

        results = executor.map(_sum, (1, 2, 3), (4, 5, 6))
        assert tuple(results) == (5, 7, 9)


@pytest.mark.parametrize(
    'config',
    (
        DaskDistributedConfig(scheduler='localhost'),
        DaskDistributedConfig(workers=1, use_threads=True, wait_for_workers=1),
    ),
)
def test_config_get_executor(config: DaskDistributedConfig) -> None:
    with mock.patch('taps.executor.dask.Client'):
        assert isinstance(config.get_executor(), DaskDistributedExecutor)
