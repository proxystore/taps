from __future__ import annotations

import enum

from pydantic import Field

from webs.config import Config


class WorkflowStructure(enum.Enum):
    """Workflow structure types."""

    BAG = 'bag'
    DIAMOND = 'diamond'
    REDUCE = 'reduce'
    SEQUENTIAL = 'sequential'


class SyntheticWorkflowConfig(Config):
    """Synthetic workflow configuration."""

    structure: WorkflowStructure = Field(description='workflow structure')
    task_count: int = Field(description='number of tasks in the workflow')
    task_data_bytes: int = Field(description='intermediate task data size')
    task_sleep: float = Field(description='minimum duration of each task')
    bag_max_running: int = Field(
        description='max running tasks in bag workflow',
    )
    warmup_task: bool = Field(
        True,
        description='submit a warmup task before running the workflow',
    )
