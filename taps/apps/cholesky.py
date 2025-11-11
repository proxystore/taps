"""Cholesky decomposition application."""

from __future__ import annotations

import logging
import pathlib
from typing import TypeAlias

import numpy
from numpy.typing import NDArray

from taps.engine import Engine
from taps.engine import task
from taps.engine import TaskFuture
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)

Array: TypeAlias = NDArray[numpy.float64]


@task()
def potrf(tile: Array) -> Array:
    """POTRF task."""
    return numpy.linalg.cholesky(tile)


@task()
def trsm(lower: Array, block: Array) -> Array:
    """TRSM task."""
    return numpy.linalg.solve(lower, block.T).T


@task()
def syrk(tile: Array, lower: Array) -> Array:
    """SYRK task."""
    return tile - numpy.dot(lower, lower.T)


@task()
def gemm(a: Array, b: Array, c: Array) -> Array:
    """GEMM task."""
    return a - numpy.dot(b, c)


def create_psd_matrix(n: int) -> Array:
    """Create a positive semi-definite matrix.

    Args:
        n: Create an `n` x `n` square matrix.

    Returns:
        Random matrix that is positive semi-definite.
    """
    psd = numpy.random.randn(n, n)
    psd = numpy.dot(psd, psd.T)
    psd += n * numpy.eye(n)
    return psd


class CholeskyApp:
    """Cholesky decomposition application.

    Computes the tiled Cholesky decomposition of a random positive-definite
    square matrix.

    Args:
        matrix_size: Matrix side length.
        block_size: Block size length.
    """

    def __init__(self, matrix_size: int, block_size: int) -> None:
        if block_size > matrix_size or matrix_size % block_size != 0:
            raise ValueError(
                'The matrix size must be greater than or equal to the block '
                'size and the block size must evenly divide the matrix size. '
                f'Got matrix_size={matrix_size} and block_size={block_size}.',
            )
        self.matrix_size = matrix_size
        self.block_size = block_size

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        max_print_size = 8

        matrix = create_psd_matrix(self.matrix_size)
        lower = numpy.zeros_like(matrix)

        n = matrix.shape[0]
        block_size = min(self.block_size, n)

        if matrix.shape[0] <= max_print_size:
            logger.log(
                APP_LOG_LEVEL,
                f'Generated input matrix: {matrix.shape}\n{matrix}',
            )
        else:
            logger.log(
                APP_LOG_LEVEL,
                f'Generated input matrix: {matrix.shape}',
            )
        logger.log(APP_LOG_LEVEL, f'Block size: {block_size}')

        for k in range(0, n, block_size):
            end_k = min(k + block_size, n)
            lower_tasks: dict[tuple[int, int], TaskFuture[Array]] = {}

            lower_tasks[(k, k)] = engine.submit(
                potrf,
                matrix[k:end_k, k:end_k],
            )

            for i in range(k + block_size, n, block_size):
                end_i = min(i + block_size, n)

                lower_tasks[(i, k)] = engine.submit(
                    trsm,
                    lower_tasks[(k, k)],
                    matrix[i:end_i, k:end_k],
                )

            gemm_tasks: dict[tuple[int, int], TaskFuture[Array]] = {}

            for i in range(k + block_size, n, block_size):
                end_i = min(i + block_size, n)
                for j in range(i, n, block_size):
                    end_j = min(j + block_size, n)

                    syrk_task = engine.submit(
                        syrk,
                        matrix[i:end_i, j:end_j],
                        lower_tasks[(i, k)],
                    )

                    gemm_tasks[(i, j)] = engine.submit(
                        gemm,
                        syrk_task,
                        lower_tasks[(i, k)],
                        lower_tasks[(j, k)],
                    )

            for (i, j), tile in lower_tasks.items():
                end_i = min(i + block_size, n)
                end_j = min(j + block_size, n)
                lower[i:end_i, j:end_j] = tile.result()

            for (i, j), tile in gemm_tasks.items():
                end_i = min(i + block_size, n)
                end_j = min(j + block_size, n)
                matrix[i:end_i, j:end_j] = tile.result()

        if matrix.shape[0] <= max_print_size:
            logger.log(APP_LOG_LEVEL, f'Output matrix:\n{lower}')
        else:
            logger.log(APP_LOG_LEVEL, f'Output matrix: {lower.shape}')
