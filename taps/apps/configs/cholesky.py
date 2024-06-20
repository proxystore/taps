from __future__ import annotations

from typing import Literal

from pydantic import Field

from taps import plugins
from taps.apps.app import App
from taps.apps.app import AppConfig


@plugins.register('app')
class CholeskyConfig(AppConfig):
    """Cholesky application configuration."""

    name: Literal['cholesky'] = 'cholesky'
    matrix_size: int = Field(description='size of the square matrix')
    block_size: int = Field(description='block size')

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.cholesky import CholeskyApp

        return CholeskyApp(
            matrix_size=self.matrix_size,
            block_size=self.block_size,
        )
