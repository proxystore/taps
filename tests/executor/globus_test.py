from __future__ import annotations

import uuid
from concurrent.futures import Executor

from testing.globus import mock_globus_compute
from webs.executor.globus import GlobusComputeConfig


def test_globus_compute_config() -> None:
    with mock_globus_compute():
        config = GlobusComputeConfig(endpoint=str(uuid.uuid4()))

        with config.get_executor() as executor:
            assert isinstance(executor, Executor)

            value = [1, 2, 3]
            future = executor.submit(sum, value)
            assert future.result() == sum(value)
