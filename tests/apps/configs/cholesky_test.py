from __future__ import annotations

import pytest
from pydantic import ValidationError

from taps.apps import App
from taps.apps.configs.cholesky import CholeskyConfig


def test_create_app() -> None:
    config = CholeskyConfig(matrix_size=4, block_size=4)
    assert isinstance(config.get_app(), App)


def test_validation_error() -> None:
    with pytest.raises(
        ValidationError,
        match='The matrix size and block size must be greater than 0.',
    ):
        CholeskyConfig(matrix_size=-1, block_size=5)

    with pytest.raises(
        ValidationError,
        match='The matrix size must be greater than or equal to the block',
    ):
        CholeskyConfig(matrix_size=1, block_size=2)
