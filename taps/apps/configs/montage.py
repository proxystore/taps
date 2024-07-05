from __future__ import annotations

import pathlib
from typing import Literal

from pydantic import Field

from taps.apps.app import App
from taps.apps.app import AppConfig
from taps.plugins import register


@register('app')
class MontageConfig(AppConfig):
    """Montage application configuration."""

    name: Literal['montage'] = 'montage'
    img_folder: pathlib.Path = Field(description='input images folder path')
    # Note: the following are annotated as str rather than pathlib.Path
    # because we don't want the AppConfig model_validator to convert
    # them to absolute paths. They are relative to whatever the run
    # directory is.
    img_tbl: str = Field(
        'Kimages.tbl',
        description='input image table filename',
    )
    img_hdr: str = Field(
        'Kimages.hdr',
        description='header filename for input images',
    )
    output_dir: str = Field(
        'data',
        description=(
            'output folder path for intermediate and final data '
            '(relative to run directory)'
        ),
    )

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.montage import MontageApp

        return MontageApp(
            img_folder=pathlib.Path(self.img_folder),
            img_tbl=self.img_tbl,
            img_hdr=self.img_hdr,
            output_dir=self.output_dir,
        )
