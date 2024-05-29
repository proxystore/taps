from __future__ import annotations

import multiprocessing
from typing import Optional

import globus_compute_sdk
from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.concurrent import ParslPoolExecutor
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors import ThreadPoolExecutor
from parsl.providers import LocalProvider
from pydantic import Field

from taps.executor.config import ExecutorConfig
from taps.executor.config import register


@register(name='parsl')
class ParslConfig(ExecutorConfig):
    """Parsl configuration.

    Attributes:
        endpoint: Globus Compute endpoint UUID.
    """

    parsl_use_threads: bool = Field(
        False,
        description=(
            'use parsl ThreadPoolExecutor instead of HighThroughputExecutor'
        ),
    )
    parsl_workers: Optional[int] = Field(None, description='max parsl workers')  # noqa: UP007
    parsl_run_dir: str = Field(
        'parsl-runinfo',
        description='parsl run directory within the workflow run directory',
    )

    def get_executor_config(self) -> Config:
        """Create a Parsl config from this config."""
        workers = (
            self.parsl_workers
            if self.parsl_workers is not None
            else multiprocessing.cpu_count()
        )

        if self.parsl_use_threads:
            executor = ThreadPoolExecutor(max_threads=workers)
        else:
            executor = HighThroughputExecutor(
                label='htex-local',
                max_workers_per_node=workers,
                address=address_by_hostname(),
                cores_per_worker=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
            )

        return Config(executors=[executor], run_dir=self.parsl_run_dir)

    def get_executor(self) -> globus_compute_sdk.Executor:
        """Create an executor instance from the config."""
        return ParslPoolExecutor(self.get_executor_config())
