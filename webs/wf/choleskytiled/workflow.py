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
from webs.wf.choleskytiled.config import CholeskytiledWorkflowConfig
from webs.workflow import register

logger = logging.getLogger(__name__)

def potrf(tile: np.ndarray) -> np.ndarray:
    return np.linalg.cholesky(tile)

def trsm(L: np.ndarray, B: np.ndarray) -> np.ndarray:
    return np.linalg.solve(L, B).T

def syrk(A: np.ndarray, L: np.ndarray) -> np.ndarray:
    return A - np.dot(L, L.T)

def gemm(A: np.ndarray, B: np.ndarray, C: np.ndarray) -> np.ndarray:
    return A - np.dot(B, C.T)
    
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
class CholeskytiledWorkflow(ContextManagerAddIn):
    """Choleskytiled workflow.

    Args:
        config: Workflow configuration.
    """

    name = 'choleskytiled'
    config_type = CholeskytiledWorkflowConfig

    def __init__(self, config: CholeskytiledWorkflowConfig) -> None:
        self.config = config
        super().__init__()

    @classmethod
    def from_config(cls, config: CholeskytiledWorkflowConfig) -> Self:
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
        # ~ matrix = create_psd_matrix(self.config.n, max_value=10)
        matrix = np.array([[4, 12, -16, 12, 37, -43, -16, -43, 98],
              [12, 37, -43, 37, 97, -123, -43, -123, 301],
              [-16, -43, 98, -43, -123, 301, 98, 301, -746]])
        lower = np.zeros_like(matrix)
        n = matrix.shape[0]
        block_size = self.config.blocksize

        # TODO: we need an assert to check that the tile size is
        # inferior to N the side of the matrix and that 
        # the tile size is > 2
        
        if matrix.shape[0] <= max_print_size:
            logger.log(WORK_LOG_LEVEL, f'Input matrix:\n{matrix}')
        else:
            logger.log(WORK_LOG_LEVEL, f'Input matrix: {matrix.shape}')

        tasks: list[TaskFuture[int]] = []

        for k in range(0, n, block_size):
            end_k = min(k + block_size, n)
        
            task = executor.submit(potrf, matrix[k:end_k, k:end_k])
            tasks.append(task)
            lower[k:end_k, k:end_k] = task.result()
        
            for i in range(k + block_size, n, block_size):
                end_i = min(i + block_size, n)
            
                task = executor.submit(trsm, lower[k:end_k, k:end_k], matrix[i:end_i, k:end_k].T)
                tasks.append(task)
                lower[i:end_i, k:end_k] = task.result()
        
            for i in range(k + block_size, n, block_size):
                end_i = min(i + block_size, n)
                for j in range(i, n, block_size):
                    end_j = min(j + block_size, n)
                
                    task = executor.submit(syrk, matrix[i:end_i, j:end_j], lower[i:end_i, k:end_k])
                    tasks.append(task)
                    matrix[i:end_i, j:end_j] = task.result()
                
                    task = executor.submit(gemm, matrix[i:end_i, j:end_j], lower[i:end_i, k:end_k], lower[j:end_j, k:end_k].T)
                    tasks.append(task)
                    matrix[i:end_i, j:end_j] = task.result()
                

        if matrix.shape[0] <= max_print_size:
            logger.log(WORK_LOG_LEVEL, f'Output matrix:\n{lower}')
        else:
            logger.log(WORK_LOG_LEVEL, f'Output matrix: {lower.shape}')
