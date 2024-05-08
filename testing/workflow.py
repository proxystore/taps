from __future__ import annotations

import pathlib
import sys
from concurrent.futures import wait

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


from webs.config import Config
from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor
from webs.workflow import register


def task() -> None:
    pass


class TestWorkflowConfig(Config):
    """Test workflow configuration."""

    tasks: int = 3


@register(name='test-workflow')
class TestWorkflow(ContextManagerAddIn):
    """Test workflow."""

    name = 'test-workflow'
    config_type = TestWorkflowConfig

    def __init__(self, tasks: int) -> None:
        self.tasks = tasks
        super().__init__()

    @classmethod
    def from_config(cls, config: TestWorkflowConfig) -> Self:
        return cls(tasks=config.tasks)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        tasks = [executor.submit(task) for _ in range(self.tasks)]
        wait([task.future for task in tasks])
