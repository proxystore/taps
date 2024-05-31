from __future__ import annotations

from pydantic import Field

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='cholesky')
class CholeskyConfig(AppConfig):
    """Cholesky application configuration."""

    matrix_size: int = Field(description='size of the square matrix')
    block_size: int = Field(description='block size')

    def create_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.cholesky import CholeskyApp

        return CholeskyApp(
            matrix_size=self.matrix_size,
            block_size=self.block_size,
        )
