from __future__ import annotations

import logging
import pathlib
import sys

import numpy as np

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from taps.context import ContextManagerAddIn
from taps.executor.workflow import TaskFuture
from taps.executor.workflow import WorkflowExecutor
from taps.logging import WORK_LOG_LEVEL
from taps.wf.cholesky.config import CholeskyWorkflowConfig

logger = logging.getLogger(__name__)


def potrf(tile: np.ndarray) -> np.ndarray:
    """POTRF task."""
    return np.linalg.cholesky(tile)


def trsm(lower: np.ndarray, block: np.ndarray) -> np.ndarray:
    """TRSM task."""
    return np.linalg.solve(lower, block.T).T


def syrk(tile: np.ndarray, lower: np.ndarray) -> np.ndarray:
    """SYRK task."""
    return tile - np.dot(lower, lower.T)


def gemm(a: np.ndarray, b: np.ndarray, c: np.ndarray) -> np.ndarray:
    """GEMM task."""
    return a - np.dot(b, c)


def create_psd_matrix(n: int) -> np.ndarray:
    """Create a positive semi-definite matrix.

    Args:
        n: Create an `n` x `n` square matrix.

    Returns:
        Random matrix that is positive semi-definite.
    """
    psd = np.random.randn(n, n)
    psd = np.dot(psd, psd.T)
    psd += n * np.eye(n)
    return psd


class CholeskyWorkflow(ContextManagerAddIn):
    """Cholesky workflow.

    Args:
        config: Workflow configuration.
    """

    name = 'cholesky'
    config_type = CholeskyWorkflowConfig

    def __init__(self, config: CholeskyWorkflowConfig) -> None:
        self.config = config
        super().__init__()

    @classmethod
    def from_config(cls, config: CholeskyWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(config)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        max_print_size = 8

        matrix = create_psd_matrix(self.config.n)
        lower = np.zeros_like(matrix)

        n = matrix.shape[0]
        block_size = min(self.config.block_size, n)

        if matrix.shape[0] <= max_print_size:
            logger.log(
                WORK_LOG_LEVEL,
                f'Input matrix: {matrix.shape}\n{matrix}',
            )
        else:
            logger.log(WORK_LOG_LEVEL, f'Input matrix: {matrix.shape}')
        logger.log(WORK_LOG_LEVEL, f'Block size: {block_size}')

        for k in range(0, n, block_size):
            end_k = min(k + block_size, n)
            lower_tasks: dict[tuple[int, int], TaskFuture[np.ndarray]] = {}

            lower_tasks[(k, k)] = executor.submit(
                potrf,
                matrix[k:end_k, k:end_k],
            )

            for i in range(k + block_size, n, block_size):
                end_i = min(i + block_size, n)

                lower_tasks[(i, k)] = executor.submit(
                    trsm,
                    lower_tasks[(k, k)],
                    matrix[i:end_i, k:end_k],
                )

            gemm_tasks: dict[tuple[int, int], TaskFuture[np.ndarray]] = {}

            for i in range(k + block_size, n, block_size):
                end_i = min(i + block_size, n)
                for j in range(i, n, block_size):
                    end_j = min(j + block_size, n)

                    syrk_task = executor.submit(
                        syrk,
                        matrix[i:end_i, j:end_j],
                        lower_tasks[(i, k)],
                    )

                    gemm_tasks[(i, j)] = executor.submit(
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
            logger.log(WORK_LOG_LEVEL, f'Output matrix:\n{lower}')
        else:
            logger.log(WORK_LOG_LEVEL, f'Output matrix: {lower.shape}')
