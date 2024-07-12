from __future__ import annotations

from typing import Literal

import globus_compute_sdk
from pydantic import Field

from taps.executor import ExecutorConfig
from taps.executor.utils import FutureDependencyExecutor
from taps.plugins import register


@register('executor')
class GlobusComputeConfig(ExecutorConfig):
    """Globus Compute configuration.

    Attributes:
        endpoint: Globus Compute endpoint UUID.
        batch_size: Maximum number of tasks to coalesce before submitting.
    """

    name: Literal['globus'] = 'globus'
    endpoint: str = Field(description='endpoint UUID')
    batch_size: int = Field(
        128,
        description='maximum number of tasks to coalesce before submitting',
    )

    def get_executor(self) -> FutureDependencyExecutor:
        """Create an executor instance from the config."""
        executor = globus_compute_sdk.Executor(
            self.endpoint,
            batch_size=self.batch_size,
        )
        return FutureDependencyExecutor(executor)
