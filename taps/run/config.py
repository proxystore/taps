from __future__ import annotations

import pathlib
from datetime import datetime
from typing import Optional
from typing import Union

import tomli_w
from pydantic import BaseModel
from pydantic import Field
from pydantic_settings import BaseSettings

from taps.apps import AppConfig
from taps.engine import AppEngineConfig


class LoggingConfig(BaseModel):
    """Logging configuration.

    Attributes:
        level: Logging level for `stdout`.
        file_level: Logging level for the log file.
        file_name: Logging file name. If `None`, only logging to `stdout`
            is used.
    """

    level: Union[int, str] = Field(  # noqa: UP007
        'INFO',
        description='minimum logging level',
    )
    file_level: Union[int, str] = Field(  # noqa: UP007
        'INFO',
        description='minimum logging level for the log file',
    )
    file_name: Optional[str] = Field(  # noqa: UP007
        'log.txt',
        description='log file name',
    )


class RunConfig(BaseModel):
    """Run configuration.

    Attributes:
        dir_format: Run directory format.
    """

    dir_format: str = Field(
        'runs/{name}-{executor}-{timestamp}',
        description=(
            'run directory format (supports "{name}", "{timestamp}", and '
            '"{executor}" for formatting)'
        ),
    )


class Config(BaseSettings):
    """Application benchmark configuration.

    Attributes:
        app: Application configuration.
        engine: Engine configuration.
        logging: Logging configuration.
        run: Run configuration.
    """

    app: AppConfig = Field(description='application configuration')
    engine: AppEngineConfig = Field(
        default_factory=AppEngineConfig,
        description='app engine configuration',
    )
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig,
        description='logging configuration',
    )
    run: RunConfig = Field(
        default_factory=RunConfig,
        description='run configuration',
    )

    def write_toml(self, filepath: str | pathlib.Path) -> None:
        """Write the configuration to a TOML file."""
        model = self.model_dump(exclude_none=True)

        filepath = pathlib.Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            tomli_w.dump(model, f)


def make_run_dir(config: Config) -> pathlib.Path:
    """Create and return the run directory path created from the config."""
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    run_dir = pathlib.Path(
        config.run.dir_format.format(
            executor=config.engine.executor,
            name=config.app.name,
            timestamp=timestamp,
        ),
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir
