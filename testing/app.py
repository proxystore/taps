from __future__ import annotations

import pathlib

from taps.app import App
from taps.app import AppConfig
from taps.engine import AppEngine


def task() -> None:
    pass


class TestAppConfig(AppConfig):
    """Test application configuration."""

    tasks: int = 3

    def create_app(self) -> App:
        return TestApp(self.tasks)


class TestApp:
    """Test application."""

    def __init__(self, tasks: int) -> None:
        self.tasks = tasks

    def close(self) -> None:
        pass

    def run(self, engine: AppEngine, run_dir: pathlib.Path) -> None:
        task_futures = [engine.submit(task) for _ in range(self.tasks)]

        for task_future in task_futures:
            task_future.result()
