from __future__ import annotations

from typing import Literal

from pydantic import Field

from taps.apps.app import App
from taps.apps.app import AppConfig
from taps.plugins import register


@register('app')
class FailinjectConfig(AppConfig):
    """Failure injection workflow configuration."""

    name: Literal['failinject'] = 'failinject'
    true_workflow: str = Field(
        'mapreduce',
        description='"cholesky", "docking", "fedlearn", "mapreduce",'
        '"montage", or "synthetic" workflow',
    )

    failure_rate: float = Field(
        1,
        description='failure rate',
    )

    failure_type: str = Field(
        'dependency',
        description='"random", "dependency", "divide_zero", "environment",'
        '"memory", "simple", "ulimit", "walltime" and TODO',
    )

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.failinject.app import FailinjectApp

        return FailinjectApp(
            true_workflow=self.true_workflow,
            failure_rate=self.failure_rate,
            failure_type=self.failure_type,
        )
