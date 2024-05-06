from __future__ import annotations

import argparse
import sys
from typing import Any
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel


class BenchmarkConfig(BaseModel):
    log_file: str
    log_level: Union[int, str]  # noqa: UP007
    log_file_level: Union[int, str]  # noqa: UP007
    run_dir: str

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title='Benchmark Harness Configuration',
        )

        group.add_argument(
            '--log-level',
            choices=['CRITICAL', 'ERROR', 'WARNING', 'BENCH', 'INFO', 'DEBUG'],
            default='INFO',
            help='Minimum logging level',
        )
        group.add_argument(
            '--log-file',
            default='log.txt',
            help='Name of log file inside --run-dir',
        )
        group.add_argument(
            '--log-file-level',
            choices=['ERROR', 'WARNING', 'BENCH', 'TEST', 'INFO', 'DEBUG'],
            default='INFO',
            help='Minimum logging level for the log file',
        )
        group.add_argument(
            '--run-dir',
            default='runs/',
            metavar='PATH',
            help=(
                'Run directory for logs and results. A subdirectory with '
                'the benchmark name and timestamp will be created for each '
                'invocation of the script.'
            ),
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        options: dict[str, Any] = {}

        if 'log_file' in kwargs:
            options['log_file'] = kwargs['log_file']
        if 'log_level' in kwargs:
            options['log_level'] = kwargs['log_level']
        if 'log_file_level' in kwargs:
            options['log_file_level'] = kwargs['log_file_level']
        if 'run_dir' in kwargs:
            options['run_dir'] = kwargs['run_dir']

        return cls(**options)
