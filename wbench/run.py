from __future__ import annotations

import argparse
import os
import sys
from typing import Any
from typing import Sequence

from wbench import wf  # noqa: F401
from wbench.config.benchmark import BenchmarkConfig
from wbench.logging import init_logging
from wbench.workflow import get_registered


def parse_args(argv: Sequence[str]) -> dict[str, Any]:
    parser = argparse.ArgumentParser(
        description='Workflow benchmark suite.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    BenchmarkConfig.add_parser_group(parser)

    subparsers = parser.add_subparsers(title='Workflows')

    workflows = get_registered()
    workflow_names = sorted(workflows.keys())
    for workflow_name in workflow_names:
        workflow = workflows[workflow_name]
        subparsers.add_parser(workflow.name)

    return vars(parser.parse_args(argv))


def main(argv: Sequence[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    args = parse_args(argv)

    benchmark_config = BenchmarkConfig.from_args(**args)
    init_logging(
        os.path.join(benchmark_config.run_dir, benchmark_config.log_file),
        benchmark_config.log_level,
        benchmark_config.log_file_level,
        force=True,
    )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
