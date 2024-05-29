from __future__ import annotations

from pydantic import Field

from taps.config import Config


class MontageWorkflowConfig(Config):
    """Montage workflow configuration."""

    img_folder: str = Field(description='input images folder path')
    img_tbl: str = Field(description='input image table file')
    img_hdr: str = Field(description='header filename for input images')
    output_dir: str = Field(description='output folder path')
