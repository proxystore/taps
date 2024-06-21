from __future__ import annotations

import pathlib
from typing import Literal

from taps import plugins
from taps.apps import App
from taps.apps import AppConfig
from taps.engine import AppEngine


def task() -> None:
    pass


@plugins.register('app')
class MockAppConfig(AppConfig):
    """Test application configuration."""

    name: Literal['mock-app'] = 'mock-app'
    tasks: int = 3

    def get_app(self) -> App:
        return MockApp(self.tasks)


class MockApp:
    """Test application."""

    def __init__(self, tasks: int) -> None:
        self.tasks = tasks

    def close(self) -> None:
        pass

    def run(self, engine: AppEngine, run_dir: pathlib.Path) -> None:
        task_futures = [engine.submit(task) for _ in range(self.tasks)]

        for task_future in task_futures:
            task_future.result()
