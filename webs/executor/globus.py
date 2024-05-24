from __future__ import annotations

import globus_compute_sdk
from pydantic import Field

from webs.executor.config import ExecutorConfig
from webs.executor.config import register
from webs.executor.dag import DAGExecutor


@register(name='globus-compute')
class GlobusComputeConfig(ExecutorConfig):
    """Globus Compute configuration.

    Attributes:
        endpoint: Globus Compute endpoint UUID.
    """

    endpoint: str = Field(description='endpoint UUID')

    def get_executor(self) -> DAGExecutor:
        """Create an executor instance from the config."""
        return DAGExecutor(globus_compute_sdk.Executor(self.endpoint))
