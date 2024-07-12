from __future__ import annotations

import multiprocessing
from typing import Literal
from typing import Optional

from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.concurrent import ParslPoolExecutor
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from pydantic import Field

from taps.executor import ExecutorConfig
from taps.plugins import register


@register('executor')
class ParslLocalConfig(ExecutorConfig):
    """Local `ParslPoolExecutor` plugin configuration.

    Simple Parsl configuration that uses the
    [`HighThroughputExecutor`][parsl.executors.HighThroughputExecutor]
    on the local node.

    Attributes:
        workers: Maximum number of Parsl workers.
        run_dir: Parsl run directory.
    """

    name: Literal['parsl-local'] = 'parsl-local'
    workers: Optional[int] = Field(None, description='max parsl workers')  # noqa: UP007
    run_dir: str = Field(
        'parsl-runinfo',
        description='parsl run directory within the app run directory',
    )

    def get_executor(self) -> ParslPoolExecutor:
        """Create an executor instance from the config."""
        workers = (
            self.workers
            if self.workers is not None
            else multiprocessing.cpu_count()
        )
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
        config = Config(executors=[executor], run_dir=self.run_dir)
        return ParslPoolExecutor(config)
