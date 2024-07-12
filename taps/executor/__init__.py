# ruff: noqa: F401
from __future__ import annotations

from taps.executor._protocol import ExecutorConfig
from taps.executor.dask import DaskDistributedConfig
from taps.executor.dask import DaskDistributedExecutor
from taps.executor.globus import GlobusComputeConfig
from taps.executor.parsl import ParslConfig
from taps.executor.python import ProcessPoolConfig
from taps.executor.python import ThreadPoolConfig
from taps.executor.ray import RayConfig
from taps.executor.ray import RayExecutor
from taps.executor.utils import FutureDependencyExecutor

__all__ = ('ExecutorConfig',)
