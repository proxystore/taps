from __future__ import annotations

from pydantic import Field

from webs.config import Config


class MontageWorkflowConfig(Config):
    """Montage workflow configuration."""

    message: str = Field(description='message to print')
    repeat: int = Field(1, description='number of times to repeat message')
