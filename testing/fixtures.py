from __future__ import annotations

import pathlib
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Generator
from unittest import mock

import pytest
from dask.distributed import Client

import taps
from taps.data.config import FilterConfig
from taps.data.null import NullTransformerConfig
from taps.executor.dask import DaskDistributedExecutor
from taps.executor.python import DAGExecutor
from taps.executor.python import ThreadPoolConfig
from taps.executor.workflow import WorkflowExecutor
from taps.run.config import BenchmarkConfig
from taps.run.config import RunConfig
from testing.app import TestAppConfig


@pytest.fixture()
def dask_executor() -> Generator[DaskDistributedExecutor, None, None]:
    client = Client(n_workers=4, processes=False, dashboard_address=None)
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
def workflow_executor(
    thread_executor: ThreadPoolExecutor,
) -> Generator[WorkflowExecutor, None, None]:
    dag_executor = DAGExecutor(thread_executor)
    with WorkflowExecutor(dag_executor) as executor:
        yield executor


@pytest.fixture()
def test_benchmark_config(
    tmp_path: pathlib.Path,
) -> Generator[BenchmarkConfig, None, None]:
    with mock.patch.dict(
        taps.run.apps.registry._REGISTERED_APP_CONFIGS,
        {'test-app': TestAppConfig},
    ):
        yield BenchmarkConfig(
            name='test-app',
            timestamp=datetime.now(),
            app=TestAppConfig(tasks=3),
            executor=ThreadPoolConfig(max_thread=4),
            transformer=NullTransformerConfig(),
            filter=FilterConfig(),
            run=RunConfig(log_file_name=None, run_dir=str(tmp_path)),
        )
