from __future__ import annotations

from concurrent.futures import Executor
from typing import Optional

from pydantic import BaseModel
from pydantic import Field

from taps.executor.dask import DaskDistributedConfig
from taps.executor.globus import GlobusComputeConfig
from taps.executor.parsl import ParslConfig
from taps.executor.python import ProcessPoolConfig
from taps.executor.python import ThreadPoolConfig
from taps.executor.ray import RayConfig


class ExecutorConfigs(BaseModel):
    """Executor configurations."""

    thread_pool: ThreadPoolConfig = Field(default_factory=ThreadPoolConfig)
    process_pool: ProcessPoolConfig = Field(default_factory=ProcessPoolConfig)
    dask: DaskDistributedConfig = Field(default_factory=DaskDistributedConfig)
    globus: Optional[GlobusComputeConfig] = Field(None)  # noqa: UP007
    parsl: ParslConfig = Field(default_factory=ParslConfig)
    ray: RayConfig = Field(default_factory=RayConfig)

    def get_executor(self, name: str) -> Executor:
        attr = name.replace('-', '_')
        config = getattr(self, attr, None)
        if config is None:
            raise ValueError(f'No executor named {name}.')
        return config.get_executor()
