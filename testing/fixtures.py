from __future__ import annotations

import pathlib
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Generator

import pytest

from testing.workflow import TestWorkflow
from testing.workflow import TestWorkflowConfig
from webs.executor.python import ThreadPoolConfig
from webs.executor.workflow import WorkflowExecutor
from webs.run.config import BenchmarkConfig
from webs.run.config import RunConfig


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
    with WorkflowExecutor(thread_executor) as executor:
        yield executor


@pytest.fixture()
def test_benchmark_config(tmp_path: pathlib.Path) -> BenchmarkConfig:
    return BenchmarkConfig(
        name=TestWorkflow.name,
        timestamp=datetime.now(),
        executor=ThreadPoolConfig(max_thread=4),
        run=RunConfig(run_dir=str(tmp_path)),
        workflow=TestWorkflowConfig(tasks=3),
    )
