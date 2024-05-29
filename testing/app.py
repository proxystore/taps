from __future__ import annotations

import pathlib
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass


from taps.apps.protocols import App
from taps.executor.workflow import WorkflowExecutor
from taps.run.apps.registry import AppConfig


def task() -> None:
    pass


class TestAppConfig(AppConfig):
    """Test workflow configuration."""

    tasks: int = 3

    def create_app(self) -> App:
        return TestApp(self.tasks)


class TestApp:
    """Test workflow."""

    def __init__(self, tasks: int) -> None:
        self.tasks = tasks

    def close(self) -> None:
        pass

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        task_futures = [executor.submit(task) for _ in range(self.tasks)]

        for task_future in task_futures:
            task_future.result()
