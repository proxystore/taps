from __future__ import annotations

from pydantic import Field
from webs.config import Config

class CholeskyWorkflowConfig(Config):
    """Cholesky workflow configuration."""

    n: int = Field(description='Size of the square matrix')
