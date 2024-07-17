from __future__ import annotations

import sys
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


@register('app')
class SyntheticConfig(AppConfig):
    """Synthetic application configuration."""

    name: Literal['synthetic'] = 'synthetic'
    structure: str = Field(description='workflow structure')
    task_count: int = Field(description='number of tasks in the workflow')
    task_data_bytes: int = Field(0, description='intermediate task data size')
    task_sleep: float = Field(0, description='minimum duration of each task')
    bag_max_running: Optional[int] = Field(  # noqa: UP007
        None,
        description='max running tasks in bag workflow',
    )
    warmup_task: bool = Field(
        True,
        description='submit a warmup task before running the workflow',
    )

    @field_validator('structure', mode='after')
    @classmethod
    def _validate_structure(cls, structure: str) -> str:
        from taps.apps.synthetic import WorkflowStructure

        try:
            WorkflowStructure(structure)
        except ValueError:
            options = ', '.join(d.value for d in WorkflowStructure)
            raise ValueError(
                f'Specified structure {structure!r} is unknown. '
                f'Must be one of {options}.',
            ) from None

        return structure

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
            warmup_task=self.warmup_task,
        )
