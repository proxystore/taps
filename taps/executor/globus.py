from __future__ import annotations

from typing import Literal

import globus_compute_sdk
from pydantic import Field

from taps.executor import ExecutorConfig
from taps.executor.utils import FutureDependencyExecutor
from taps.plugins import register


@register('executor')
class GlobusComputeConfig(ExecutorConfig):
    """Globus Compute [`Executor`][globus_compute_sdk.Executor] plugin configuration."""  # noqa: E501

    name: Literal['globus'] = Field('globus', description='Executor name.')
    endpoint: str = Field(description='Globus Compute Endpoint UUID.')
    batch_size: int = Field(
        128,
        description='Maximum number of tasks to coalesce before submitting.',
    )

    def get_executor(self) -> FutureDependencyExecutor:
        """Create an executor instance from the config."""
        executor = globus_compute_sdk.Executor(
            self.endpoint,
            batch_size=self.batch_size,
        )
        return FutureDependencyExecutor(executor)
