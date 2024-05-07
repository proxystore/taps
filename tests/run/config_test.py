from __future__ import annotations

from webs.run.config import BenchmarkConfig


def test_benchmark_config_paths(
    test_benchmark_config: BenchmarkConfig,
) -> None:
    test_benchmark_config.get_run_dir().exists()
    assert test_benchmark_config.get_log_file() is None
    test_benchmark_config.run.log_file_name = 'log.txt'
    assert test_benchmark_config.get_log_file() is not None
