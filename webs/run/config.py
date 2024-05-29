from __future__ import annotations

import pathlib
from datetime import datetime
from typing import Optional
from typing import Union

from pydantic import Field
from pydantic import SerializeAsAny

from webs.config import Config
from webs.data.config import FilterConfig
from webs.data.config import TransformerConfig
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
        'runs/{name}-{timestamp}',
        description=(
            'run directory format (supports "{name}" and "{timestamp}" '
            'for formatting)'
        ),
    )


class BenchmarkConfig(Config):
    """Workflow benchmark configuration.

    Attributes:
        name: Name of the workflow to execute.
        timestamp: Start time of the workflow.
        transformer: Transformer config.
        filter: Filter config.
        run: Run configuration.
        workflow: Workflow configuration.
    """

    name: str
    timestamp: datetime
    executor: SerializeAsAny[ExecutorConfig]
    transformer: SerializeAsAny[TransformerConfig]
    filter: SerializeAsAny[FilterConfig]
    run: SerializeAsAny[RunConfig]
    workflow: SerializeAsAny[Config]

    def get_run_dir(self) -> pathlib.Path:
        """Create and return the path to the run directory."""
        timestamp = self.timestamp.strftime('%Y-%m-%d-%H-%M-%S')
        run_dir = pathlib.Path(
            self.run.run_dir_format.format(
                name=self.name,
                timestamp=timestamp,
            ),
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
