from __future__ import annotations

import globus_compute_sdk
from pydantic import Field

from taps.executor.config import ExecutorConfig
from taps.executor.config import register
from taps.executor.dag import DAGExecutor


@register(name='globus-compute')
class GlobusComputeConfig(ExecutorConfig):
    """Globus Compute configuration.

    Attributes:
        endpoint: Globus Compute endpoint UUID.
    """

    globus_compute_endpoint: str = Field(description='endpoint UUID')
    globus_compute_batch_size: int = Field(
        128,
        description='maximum number of tasks to coalesce before submitting',
    )

    def get_executor(self) -> DAGExecutor:
        """Create an executor instance from the config."""
        executor = globus_compute_sdk.Executor(
            self.globus_compute_endpoint,
            batch_size=self.globus_compute_batch_size,
        )
        return DAGExecutor(executor)
