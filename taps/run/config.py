from __future__ import annotations

import pathlib
from datetime import datetime
from typing import Optional
from typing import Union

from pydantic import Field
from pydantic import SerializeAsAny

from taps.config import Config
from taps.data.config import DataTransformerConfig
from taps.data.config import FilterConfig
from taps.executor.config import ExecutorConfig
from taps.run.apps.registry import AppConfig


class RunConfig(Config):
    """Run configuration.

    Attributes:
        log_file_level: Logging level for the log file.
        log_file_name: Logging file name. If `None`, only logging to `stdout`
            is used.
        log_level: Logging level for `stdout`.
        run_dir: Runtime directory.
    """

    log_file_level: Union[int, str] = Field(  # noqa: UP007
        'INFO',
        description='minimum logging level for the log file',
    )
    log_file_name: Optional[str] = Field(  # noqa: UP007
        'log.txt',
        description='log file name',
    )
    log_level: Union[int, str] = Field(  # noqa: UP007
        'INFO',
        description='minimum logging level',
    )
    task_record_file_name: str = Field(
        'tasks.jsonl',
        description='task record line-delimited JSON file name',
    )
    run_dir_format: str = Field(
        'runs/{name}-{executor}-{timestamp}',
        description=(
            'run directory format (supports "{name}", "{timestamp}", and '
            '"{executor}" for formatting)'
        ),
    )


class BenchmarkConfig(Config):
    """Application benchmark configuration.

    Attributes:
        name: Name of the application to execute.
        timestamp: Start time of the application.
        executor_name: Name of the executor.
        app: Application config.
        executor: Executor config.
        filter: Filter config.
        run: Run config.
        transformer: Transformer config.
    """

    name: str
    timestamp: datetime
    executor_name: str
    app: SerializeAsAny[AppConfig]
    executor: SerializeAsAny[ExecutorConfig]
    filter: SerializeAsAny[FilterConfig]
    run: SerializeAsAny[RunConfig]
    transformer: SerializeAsAny[DataTransformerConfig]

    def get_run_dir(self) -> pathlib.Path:
        """Create and return the path to the run directory."""
        timestamp = self.timestamp.strftime('%Y-%m-%d-%H-%M-%S')
        run_dir = pathlib.Path(
            self.run.run_dir_format.format(
                executor=self.executor_name,
                name=self.name,
                timestamp=timestamp,
            ),
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
