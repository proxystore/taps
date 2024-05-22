from __future__ import annotations

from webs.executor.ray import RayConfig


def test_ray_executor() -> None:
    config = RayConfig(ray_address='local', ray_num_cpus=2)
    executor = config.get_executor()

    with executor:
        future = executor.submit(sum, [1, 2], start=-3)
        assert future.result() == 0

        future = executor.submit(sum, [1, 2, 3], start=-6)
        assert future.result() == 0

        output = executor.map(abs, [1, -1])
        assert list(output) == [1, 1]
