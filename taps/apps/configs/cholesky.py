from __future__ import annotations

import sys
from typing import Literal

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import Field
from pydantic import model_validator

from taps.apps import App
from taps.apps import AppConfig
from taps.plugins import register


@register('app')
class CholeskyConfig(AppConfig):
    """Cholesky application configuration."""

    name: Literal['cholesky'] = 'cholesky'
    matrix_size: int = Field(description='size of the square matrix')
    block_size: int = Field(description='block size')

    @model_validator(mode='after')
    def _validate_sizes(self) -> Self:
        if self.matrix_size <= 0 or self.block_size <= 0:
            raise ValueError(
                'The matrix size and block size must be greater than 0. '
                f'Got matrix_size={self.matrix_size} and '
                f'block_size={self.block_size}.',
            )

        if (
            self.block_size > self.matrix_size
            or self.matrix_size % self.block_size != 0
        ):
            raise ValueError(
                'The matrix size must be greater than or equal to the block '
                'size and the block size must evenly divide the matrix size. '
                f'Got matrix_size={self.matrix_size} and '
                f'block_size={self.block_size}.',
            )

        return self

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.cholesky import CholeskyApp

        return CholeskyApp(
            matrix_size=self.matrix_size,
            block_size=self.block_size,
        )
