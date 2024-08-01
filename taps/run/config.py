from __future__ import annotations

import pathlib
import sys
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    import tomllib
else:  # pragma: <3.11 cover
    import tomli as tomllib

import tomli_w
from pydantic import BaseModel
from pydantic import create_model
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

import taps
from taps.apps import AppConfig
from taps.engine import EngineConfig
from taps.plugins import get_app_configs
from taps.plugins import get_executor_configs
from taps.plugins import get_filter_configs
from taps.plugins import get_transformer_configs
from taps.run.utils import flatten_mapping


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
        env_vars: Dictionary mapping environment variables to values.
            The environment variables will be set once the benchmark starts.
    """

    dir_format: str = Field(
        'runs/{name}_{executor}_{timestamp}',
        description=(
            'run directory format (supports "{name}", "{timestamp}", and '
            '"{executor}" for formatting)'
        ),
    )
    env_vars: Dict[str, str] = Field(  # noqa: UP006
        default_factory=dict,
        description='environment variables to set during benchmarking',
    )


class Config(BaseSettings):
    """Application benchmark configuration.

    Attributes:
        app: Application configuration.
        engine: Engine configuration.
        logging: Logging configuration.
        run: Run configuration.
        version: TaPS version used to create the config. Loading a config
            with a version that does not match the current version will
            log a warning that behavior could be different.
    """

    model_config = SettingsConfigDict(
        extra='forbid',
        validate_default=True,
        validate_return=True,
    )

    app: AppConfig = Field(description='application configuration')
    engine: EngineConfig = Field(default_factory=EngineConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    run: RunConfig = Field(default_factory=RunConfig)
    version: str = Field(
        taps.__version__,
        description='TaPS version (do not alter)',
    )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Config):
            raise NotImplementedError(
                'Config equality is not implemented for non-Config types.',
            )

        # Equality is manually implemented because in Pydantic v2,
        # equality, by default, requires the two model instances to be
        # of the same type, but sometimes we dynamically create new Config
        # types.
        return (
            self.app == other.app
            and self.engine == other.engine
            and self.logging == other.logging
            and self.run == other.run
            and self.version == other.version
        )

    @classmethod
    def from_toml(cls, filepath: str | pathlib.Path) -> Config:
        """Load a configuration from a TOML file."""
        with open(filepath, 'rb') as f:
            options = tomllib.load(f)

        flat_options = flatten_mapping(options)
        config_cls = _make_config_cls(flat_options)

        return config_cls(**options)

    def write_toml(self, filepath: str | pathlib.Path) -> None:
        """Write the configuration to a TOML file."""
        model = self.model_dump(
            exclude_unset=False,
            exclude_defaults=False,
            exclude_none=True,
        )

        filepath = pathlib.Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            tomli_w.dump(model, f)


def _make_config_cls(options: dict[str, Any]) -> type[Config]:
    # The Config and EngineConfig BaseModels contain attributes that
    # are typed using ABCs. Thus, CliSettingsSource cannot infer the
    # CLI options to add from the ABC. To get around this, we dynamically
    # create a new Config/EngineConfig using concrete types based on
    # the plugin names provided by the user.
    app_name = options.get('app.name')
    assert isinstance(app_name, str)
    app_cls = get_app_configs()[app_name]
    app_field = Field(description=f'selected app: {app_name}')

    executor_name = options.get('engine.executor.name', 'process-pool')
    executor_cls = get_executor_configs()[executor_name]
    executor_field = Field(
        default_factory=executor_cls,
        description=f'selected executor: {executor_name}',
    )

    filter_name = options.get('engine.filter.name', 'all')
    filter_cls = get_filter_configs()[filter_name]
    filter_field = Field(
        default_factory=filter_cls,
        description=f'selected filter: {filter_name}',
    )

    transformer_name = options.get('engine.transformer.name', 'null')
    transformer_cls = get_transformer_configs()[transformer_name]
    transformer_field = Field(
        default_factory=transformer_cls,
        description=f'selected transformer: {transformer_name}',
    )

    engine_cls = create_model(
        'EngineConfig',
        executor=(executor_cls, executor_field),
        filter=(filter_cls, filter_field),
        transformer=(transformer_cls, transformer_field),
        __base__=EngineConfig,
        __doc__=EngineConfig.__doc__,
    )
    engine_field = Field(
        default_factory=engine_cls,
        description='engine options',
    )

    return create_model(
        'GeneratedConfig',
        app=(app_cls, app_field),
        engine=(engine_cls, engine_field),
        __base__=Config,
        __doc__=Config.__doc__,
    )


def make_run_dir(config: Config) -> pathlib.Path:
    """Create and return the run directory path created from the config."""
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    run_dir = pathlib.Path(
        config.run.dir_format.format(
            executor=config.engine.executor.name,
            name=config.app.name,
            timestamp=timestamp,
        ),
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir.resolve()
