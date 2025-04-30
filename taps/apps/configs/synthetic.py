from __future__ import annotations

import enum
import sys
from typing import Any
from typing import Literal
from typing import Optional

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from taps.apps import App
from taps.apps import AppConfig
from taps.plugins import register


class WorkflowStructure(enum.Enum):
    """Workflow structure types."""

    BAG = 'bag'
    DIAMOND = 'diamond'
    REDUCE = 'reduce'
    SEQUENTIAL = 'sequential'


@register('app')
class SyntheticConfig(AppConfig, use_enum_values=True):
    """Synthetic application configuration."""

    name: Literal['synthetic'] = Field(
        'synthetic',
        description='Application name.',
    )
    structure: WorkflowStructure = Field(description='Workflow structure.')
    task_count: int = Field(description='Number of tasks in the workflow.')
    task_data_bytes: int = Field(0, description='Intermediate task data size.')
    task_sleep: float = Field(0, description='Minimum duration of each task.')
    bag_max_running: Optional[int] = Field(  # noqa: UP007
        None,
        description='Max running tasks in bag workflow.',
    )
    warmup_tasks: int = Field(
        0,
        description='Number of warmup tasks before running the workflow.',
    )
    task_std: float = Field(0, description='Standard deviation in duration.')

    @field_validator('structure', mode='before')
    @classmethod
    def _validate_structure(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.lower()
        return value

    @model_validator(mode='after')
    def _validate_options(self) -> Self:
        from taps.apps.synthetic import WorkflowStructure

        structure = WorkflowStructure(self.structure)
        if structure == WorkflowStructure.BAG and self.bag_max_running is None:
            raise ValueError(
                "Option 'bag_max_running' must be specified when "
                f'{WorkflowStructure.BAG.value!r} is specified.',
            )

        return self

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.synthetic import SyntheticApp
        from taps.apps.synthetic import WorkflowStructure

        return SyntheticApp(
            structure=WorkflowStructure(self.structure),
            task_count=self.task_count,
            task_data_bytes=self.task_data_bytes,
            task_sleep=self.task_sleep,
            bag_max_running=self.bag_max_running,
            warmup_tasks=self.warmup_tasks,
            task_std=self.task_std,
        )
