from __future__ import annotations

import pathlib
from typing import Literal

from pydantic import Field

from taps.apps import App
from taps.apps import AppConfig
from taps.engine import Engine
from taps.engine import task
from taps.plugins import register


@task()
def mock_app_task() -> None:
    pass


@register('app')
class MockAppConfig(AppConfig):
    """Test application configuration."""

    name: Literal['mock-app'] = 'mock-app'
    tasks: int = Field(3, description='number of tasks to perform')

    def get_app(self) -> App:
        return MockApp(self.tasks)


class MockApp:
    """Test application."""

    def __init__(self, tasks: int) -> None:
        self.tasks = tasks

    def close(self) -> None:
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        task_futures = [
            engine.submit(mock_app_task) for _ in range(self.tasks)
        ]

        for task_future in task_futures:
            task_future.result()
