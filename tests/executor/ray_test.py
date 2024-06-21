from __future__ import annotations

import sys

import pytest

from taps.executor.ray import RayConfig


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason='Ray wheels for Python 3.12 are not available',
)
def test_ray_executor() -> None:
    config = RayConfig(address='local', num_cpus=2)
    executor = config.get_executor()

    with executor:
        future = executor.submit(sum, [1, 2], start=-3)
        assert future.result() == 0

        future = executor.submit(sum, [1, 2, 3], start=-6)
        assert future.result() == 0

        output = executor.map(abs, [1, -1])
        assert list(output) == [1, 1]
