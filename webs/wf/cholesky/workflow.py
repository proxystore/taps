from __future__ import annotations

import logging
import math
import pathlib
import sys

import numpy as np

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import TaskFuture
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.cholesky.config import CholeskyWorkflowConfig
from webs.workflow import register

logger = logging.getLogger(__name__)


def potrf_task(lower_j_k: int) -> int:
    """Potrf task."""
    return pow(lower_j_k, 2)


def agregator_task(matrix_j_j: int, sum1: int) -> int:
    """Aggregator task.

    ```python
    lower[j][j] = int(math.sqrt(matrix[j][j] - sum1))
    ```
    """
    return int(math.sqrt(matrix_j_j - sum1))


def trsm_task(
    matrix_i_j: int,
    sum1: int,
    lower_j_j: int,
) -> int:
    """Trsm task.

    ```python
    lower[i][j] = int((matrix[i][j] - sum1)/lower[j][j])
    ```
    """
    return int((matrix_i_j - sum1) / lower_j_j)


def gemm_task(lower_i_k: int, lower_j_k: int) -> int:
    """Gemm task.

    ```python
    sum += (lower[i][k] * lower[j][k]);
    ```
    """
    return lower_i_k * lower_j_k


def create_psd_matrix(n: int, max_value: int = 100) -> np.ndarray:
    """Create a positive semi-definite matrix.

    Args:
        n: Create an `n` x `n` square matrix.
        max_value: Maximum element value.

    Returns:
        Random matrix that is positive semi-definite.
    """
    max_value = max_value // 2
    left = np.tril(np.random.randint(1, max_value + 1, size=(n, n)))
    return np.dot(left, left.T)


@register()
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
        max_print_size = 10
        matrix = create_psd_matrix(self.config.n, max_value=10)
        lower = np.zeros_like(matrix)

        if matrix.shape[0] <= max_print_size:
            logger.log(WORK_LOG_LEVEL, f'Input matrix:\n{matrix}')
        else:
            logger.log(WORK_LOG_LEVEL, f'Input matrix: {matrix.shape}')

        tasks: list[TaskFuture[int]] = []

        for i in range(self.config.n):
            for j in range(i + 1):
                sum1 = 0
                if j == i:
                    for k in range(j):
                        task = executor.submit(potrf_task, lower[j][k])
                        tasks.append(task)
                        sum1 += task.result()
                    # sum(task.result() for task in tasks)
                    # ~ sum1 = executor.map(potf_task, list(range(j), lower)
                    task = executor.submit(agregator_task, matrix[j][j], sum1)
                    tasks.append(task)
                    lower[j][j] = task.result()
                else:
                    for k in range(j):
                        task = executor.submit(
                            gemm_task,
                            lower[i][k],
                            lower[j][k],
                        )
                        tasks.append(task)
                        sum1 += task.result()
                    if lower[j][j] > 0:
                        task = executor.submit(
                            trsm_task,
                            matrix[i][j],
                            sum1,
                            lower[j][j],
                        )
                        tasks.append(task)
                        lower[i][j] = task.result()

        if matrix.shape[0] <= max_print_size:
            logger.log(WORK_LOG_LEVEL, f'Output matrix:\n{lower}')
        else:
            logger.log(WORK_LOG_LEVEL, f'Output matrix: {lower.shape}')
