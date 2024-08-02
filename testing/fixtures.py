from __future__ import annotations

import pathlib
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Generator

import pytest
from dask.distributed import Client

from taps.engine import Engine
from taps.engine import EngineConfig
from taps.executor import DaskDistributedExecutor
from taps.executor import FutureDependencyExecutor
from taps.executor import ThreadPoolConfig
from taps.filter import NullFilterConfig
from taps.run.config import Config
from taps.run.config import LoggingConfig
from taps.run.config import RunConfig
from taps.transformer import NullTransformerConfig
from testing.app import MockAppConfig


@pytest.fixture()
def dask_executor() -> Generator[DaskDistributedExecutor, None, None]:
    client = Client(
        n_workers=4,
        processes=False,
        dashboard_address=None,
        worker_dashboard_address=None,
    )
    with DaskDistributedExecutor(client) as executor:
        yield executor


@pytest.fixture()
def dask_process_executor() -> Generator[DaskDistributedExecutor, None, None]:
    client = Client(
        n_workers=4,
        processes=True,
        dashboard_address=None,
        worker_dashboard_address=None,
    )
    with DaskDistributedExecutor(client) as executor:
        yield executor


@pytest.fixture()
def process_executor() -> Generator[ProcessPoolExecutor, None, None]:
    with ProcessPoolExecutor(4) as executor:
        yield executor


@pytest.fixture()
def thread_executor() -> Generator[ThreadPoolExecutor, None, None]:
    with ThreadPoolExecutor(4) as executor:
        yield executor


@pytest.fixture()
def engine(
    thread_executor: ThreadPoolExecutor,
) -> Generator[Engine, None, None]:
    dag_executor = FutureDependencyExecutor(thread_executor)
    with Engine(dag_executor) as executor:
        yield executor


@pytest.fixture()
def test_benchmark_config(tmp_path: pathlib.Path) -> Config:
    return Config(
        app=MockAppConfig(tasks=3),
        engine=EngineConfig(
            executor=ThreadPoolConfig(max_threads=4),
            filter=NullFilterConfig(),
            transformer=NullTransformerConfig(),
            task_record_file_name=None,
        ),
        logging=LoggingConfig(file_name=None),
        run=RunConfig(dir_format=str(tmp_path.resolve())),
    )
