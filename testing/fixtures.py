from __future__ import annotations

import pathlib
from datetime import datetime

import pytest

from testing.workflow import TestWorkflow
from testing.workflow import TestWorkflowConfig
from webs.executor.python import ThreadPoolConfig
from webs.run.config import BenchmarkConfig
from webs.run.config import RunConfig


@pytest.fixture()
def test_benchmark_config(tmp_path: pathlib.Path) -> BenchmarkConfig:
    return BenchmarkConfig(
        name=TestWorkflow.name,
        timestamp=datetime.now(),
        executor=ThreadPoolConfig(max_thread=4),
        run=RunConfig(run_dir=str(tmp_path)),
        workflow=TestWorkflowConfig(tasks=3),
    )
