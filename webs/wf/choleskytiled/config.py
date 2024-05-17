from __future__ import annotations

from pydantic import Field

from webs.config import Config


class CholeskytiledWorkflowConfig(Config):
    """Choleskytiled workflow configuration."""

    n: int = Field(description='size of the square matrix')
    blocksize: int = Field(description='block size')
