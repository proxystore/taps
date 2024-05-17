from __future__ import annotations

from pydantic import Field

from webs.config import Config


class CholeskyWorkflowConfig(Config):
    """Cholesky workflow configuration."""

    n: int = Field(description='size of the square matrix')
    block_size: int = Field(description='block size')
