from __future__ import annotations

from pydantic import Field

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='montage')
class MontageConfig(AppConfig):
    """Montage application configuration."""

    img_folder: str = Field(description='input images folder path')
    img_tbl: str = Field(description='input image table file')
    img_hdr: str = Field(description='header filename for input images')
    output_dir: str = Field(description='output folder path')

    def create_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.montage import MontageApp

        return MontageApp(
            img_folder=self.img_folder,
            img_tbl=self.img_tbl,
            img_hdr=self.img_hdr,
            output_dir=self.output_dir,
        )
