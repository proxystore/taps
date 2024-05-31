from __future__ import annotations

from pydantic import Field
from pydantic import field_validator

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='synthetic')
class SyntheticConfig(AppConfig):
    """Synthetic application configuration."""

    structure: str = Field(description='workflow structure')
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

    @field_validator('structure', mode='after')
    @classmethod
    def _validate_structure(cls, structure: str) -> str:
        from taps.apps.synthetic import WorkflowStructure

        try:
            WorkflowStructure(structure)
        except KeyError:
            options = ', '.join(d.value for d in WorkflowStructure)
            raise ValueError(
                f'{structure} is not a supported structure. '
                f'Must be one of {options}.',
            ) from None

        return structure

    def create_app(self) -> App:
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
