from __future__ import annotations

import pathlib
from typing import Literal
from typing import Optional

from pydantic import Field

from taps.apps.app import App
from taps.apps.app import AppConfig
from taps.plugins import register


@register('app')
class MapreduceConfig(AppConfig):
    """Mapreduce application configuration."""

    name: Literal['mapreduce'] = 'mapreduce'
    data_dir: pathlib.Path = Field(description='text file directory')
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
