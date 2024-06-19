from __future__ import annotations

import pathlib
import sys
from datetime import datetime
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import tomli_w
from pydantic import BaseModel
from pydantic import create_model
from pydantic import Field
from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import CliSettingsSource
from pydantic_settings import PydanticBaseSettingsSource
from pydantic_settings import TomlConfigSettingsSource

from taps.app import App
from taps.engine import AppEngineConfig
from taps.run.apps.registry import get_registered_apps


class BaseAppConfigs(BaseModel):
    name: str

    @model_validator(mode='after')
    def _validate_model(self) -> Self:
        if not hasattr(self, self.name):
            raise ValueError(
                f'{type(self).__name__} has no application named {self.name}.',
            )
        if getattr(self, self.name) is None:
            app_type = get_registered_apps()[self.name]
            raise ValueError(
                f'The specified app is {self.name} but the configuration '
                f'parameters for {app_type} were not provided.',
            )
        return self

    def get_app(self) -> App:
        attr = self.name.replace('-', '_')
        config = getattr(self, attr, None)
        if config is None:
            raise ValueError(f'No application named {self.name}')
        return config.get_app()


_app_fields = {
    name: (type_, Field(None)) for name, type_ in get_registered_apps().items()
}

if TYPE_CHECKING:
    # mypy can't infer the dynamically generated AppConfigs
    AppConfigs = BaseAppConfigs
else:
    AppConfigs = create_model('Apps', **_app_fields, __base__=BaseAppConfigs)


class LoggingConfig(BaseModel):
    """Run configuration.

    Attributes:
        log_file_level: Logging level for the log file.
        log_file_name: Logging file name. If `None`, only logging to `stdout`
            is used.
        log_level: Logging level for `stdout`.
        run_dir: Runtime directory.
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


class OutputConfig(BaseModel):
    run_dir_format: str = Field(
        'runs/{name}-{executor}-{timestamp}',
        description=(
            'run directory format (supports "{name}", "{timestamp}", and '
            '"{executor}" for formatting)'
        ),
    )


class Config(BaseSettings):
    """Application benchmark configuration.

    Attributes:
        config: Optional path to TOML file containing the base configuration
            options.
        app: Application configuration.
        engine: Engine configuration.
        logging: Logging configuration.
        output: Output configuration.
    """

    config: Optional[pathlib.Path] = Field(None)  # noqa: UP007
    app: AppConfigs
    engine: AppEngineConfig = Field(default_factory=AppEngineConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, PydanticBaseSettingsSource]:
        toml_file: str | None = None
        for i, arg in enumerate(sys.argv):
            if not arg.startswith('--config'):
                continue
            if arg.startswith('--config='):
                toml_file = arg.split('=')[1]
            else:
                toml_file = sys.argv[i + 1]

        return (
            CliSettingsSource(settings_cls, cli_parse_args=True),
            TomlConfigSettingsSource(settings_cls, toml_file=toml_file),
        )

    def write_toml(self, filepath: str | pathlib.Path) -> None:
        model = self.model_dump(exclude_none=True)

        filepath = pathlib.Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            tomli_w.dump(model, f)


def make_run_dir(config: Config) -> pathlib.Path:
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    run_dir = pathlib.Path(
        config.output.run_dir_format.format(
            executor=config.engine.executor,
            name=config.app.name,
            timestamp=timestamp,
        ),
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir
