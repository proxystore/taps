from __future__ import annotations

from taps.executor.ray import RayConfig


def test_ray_executor() -> None:
    config = RayConfig(address='local', num_cpus=2)
    executor = config.get_executor()
    assert isinstance(repr(executor), str)

    with executor:
        future = executor.submit(sum, [1, 2], start=-3)
        assert future.result() == 0

        future = executor.submit(sum, [1, 2, 3], start=-6)
        assert future.result() == 0

        output = executor.map(abs, [1, -1])
        assert list(output) == [1, 1]
