from __future__ import annotations

import logging
import pathlib
import sys
from concurrent.futures import Executor

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


from webs.config import Config
from webs.context import ContextManagerAddIn
from webs.workflow import register

logger = logging.getLogger('template')


class TemplateConfig(Config):
    """Template workflow configuration."""

    input_bytes: int


@register(name='template')
class TemplateWorkflow(ContextManagerAddIn):
    """Template workflow."""

    name = 'template'
    config_type = TemplateConfig

    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def from_config(cls, config: TemplateConfig) -> Self:
        """Create a workflow instance from a config."""
        return cls()

    def run(
        self,
        executor: Executor,
        run_dir: pathlib.Path,
    ) -> None:
        """Run the workflow."""
        logger.info('Submitting task')
        future = executor.submit(sum, [1, 2, 3])
        logger.info(f'Result: {future.result()}')
