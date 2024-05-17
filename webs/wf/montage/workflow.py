from __future__ import annotations

import logging
import pathlib
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.montage.config import MontageWorkflowConfig
from webs.workflow import register

logger = logging.getLogger(__name__)


def print_message(message: str) -> None:
    """Print a message."""
    logger.log(WORK_LOG_LEVEL, message)


@register()
class MontageWorkflow(ContextManagerAddIn):
    """Montage workflow.

    Args:
        message: Message to print.
        repeat: Number of times to repeat the message.
    """

    name = 'montage'
    config_type = MontageWorkflowConfig

    def __init__(self, message: str, repeat: int = 1) -> None:
        self.message = message
        self.repeat = repeat
        super().__init__()

    @classmethod
    def from_config(cls, config: MontageWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(message=config.message, repeat=config.repeat)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        for _ in range(self.repeat):
            task = executor.submit(print_message, self.message)
            task.result()  # Wait on task to finish
