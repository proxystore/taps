from __future__ import annotations

from pydantic import Field

from webs.config import Config


class DockingWorkflowConfig(Config):
    """Synthetic workflow configuration."""

    task_count: int = Field(description='number of tasks in the workflow')
    task_data_bytes: int = Field(description='intermediate task data size')
    task_sleep: float = Field(description='minimum duration of each task')
