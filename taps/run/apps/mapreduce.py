from __future__ import annotations

import pathlib
from typing import Literal
from typing import Optional

from pydantic import Field
from pydantic import field_validator

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='mapreduce')
class MapreduceConfig(AppConfig):
    """Mapreduce application configuration."""

    name: Literal['mapreduce'] = 'mapreduce'
    data_dir: str = Field(description='text file directory')
    map_tasks: Optional[int] = Field(  # noqa: UP007
        32,
        description=(
            'maximum number of map tasks (none, the default, uses one '
            'map task per input file)'
        ),
    )
    generate: bool = Field(
        False,
        description=(
            'generate random text files in --data-dir rather than '
            'reading existing files'
        ),
    )
    generated_files: int = Field(
        10,
        description='number of text files to generate',
    )
    generated_words: int = Field(
        10000,
        description='number of words to generate per file',
    )

    @field_validator('data_dir', mode='before')
    @classmethod
    def _resolve_paths(cls, path: str) -> str:
        return str(pathlib.Path(path).resolve())

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.mapreduce import MapreduceApp

        return MapreduceApp(
            data_dir=pathlib.Path(self.data_dir),
            map_tasks=self.map_tasks,
            generate=self.generate,
            generated_files=self.generated_files,
            generated_words=self.generated_words,
        )
