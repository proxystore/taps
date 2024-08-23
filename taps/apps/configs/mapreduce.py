from __future__ import annotations

import pathlib
from typing import Literal
from typing import Optional

from pydantic import Field

from taps.apps import App
from taps.apps import AppConfig
from taps.plugins import register


@register('app')
class MapreduceConfig(AppConfig):
    """Mapreduce application configuration."""

    name: Literal['mapreduce'] = Field(
        'mapreduce',
        description='Application name.',
    )
    data_dir: pathlib.Path = Field(description='Text file directory.')
    map_tasks: Optional[int] = Field(  # noqa: UP007
        32,
        description=(
            'Maximum number of map tasks (`None` uses one '
            'map task per input file).'
        ),
    )
    generate: bool = Field(
        False,
        description=(
            'Generate random text files in data-dir rather than '
            'reading existing files.'
        ),
    )
    generated_files: int = Field(
        10,
        description='Number of text files to generate.',
    )
    generated_words: int = Field(
        10000,
        description='Number of words to generate per file.',
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
