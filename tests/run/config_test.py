from __future__ import annotations

from webs.run.config import BenchmarkConfig


def test_benchmark_config_paths(
    test_benchmark_config: BenchmarkConfig,
) -> None:
    test_benchmark_config.get_run_dir().exists()
