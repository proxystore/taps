from __future__ import annotations

import pathlib

import pytest

from taps.apps.cholesky import CholeskyApp
from taps.engine import Engine


@pytest.mark.parametrize(('matrix_size', 'block_size'), ((4, 4), (16, 4)))
def test_cholesky_app(
    matrix_size: int,
    block_size: int,
    engine: Engine,
    tmp_path: pathlib.Path,
) -> None:
    app = CholeskyApp(matrix_size, block_size)
    app.run(engine, tmp_path)
    app.close()


def test_mismatched_size_error() -> None:
    with pytest.raises(
        ValueError,
        match='The matrix size must be greater than or equal to the block',
    ):
        CholeskyApp(16, 5)
