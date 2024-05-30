from __future__ import annotations

import pathlib

from taps.app import App
from taps.app import AppConfig
from taps.executor.workflow import WorkflowExecutor


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
