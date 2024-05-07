from __future__ import annotations

import pathlib
from datetime import datetime
from typing import Optional
from typing import Union

from pydantic import Field

from webs.config import Config
from webs.executor.config import ExecutorConfig


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
        None,
        description='log file name',
    )
    log_level: Union[int, str] = Field(  # noqa: UP007
        'INFO',
        description='minimum logging level',
    )
    run_dir: str = Field(
        'runs/{name}-{timestamp}',
        description='run directory',
    )


class BenchmarkConfig(Config):
    """Workflow benchmark configuration.

    Attributes:
        name: Name of the workflow to execute.
        timestamp: Start time of the workflow.
        run: Run configuration.
        workflow: Workflow configuration.
    """

    name: str
    timestamp: datetime
    executor: ExecutorConfig
    run: RunConfig
    workflow: Config

    def get_log_file(self) -> pathlib.Path | None:
        """Get the log file if specified."""
        log_file_name = self.run.log_file_name
        if log_file_name is None:
            return None
        return self.get_run_dir() / log_file_name

    def get_run_dir(self) -> pathlib.Path:
        """Create and return the path to the run directory."""
        timestamp = self.timestamp.strftime('%Y-%m-%d-%H-%M-%S')
        run_dir = pathlib.Path(
            self.run.run_dir.format(name=self.name, timestamp=timestamp),
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
