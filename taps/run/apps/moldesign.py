from __future__ import annotations

import pathlib

from pydantic import Field
from pydantic import field_validator

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='moldesign')
class MoldesignConfig(AppConfig):
    """Moldesign application configuration."""

    dataset: str = Field(description='molecule search space dataset')
    initial_count: int = Field(8, description='number of initial calculations')
    search_count: int = Field(
        64,
        description='number of molecules to evaluate in total',
    )
    batch_size: int = Field(
        4,
        description=(
            'number of molecules to evaluate in each batch of simulations'
        ),
    )
    seed: int = Field(0, description='random seed')

    @field_validator('dataset', mode='before')
    @classmethod
    def _resolve_dataset_path(cls, value: str) -> str:
        return str(pathlib.Path(value).resolve())

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.moldesign.app import MoldesignApp

        return MoldesignApp(
            dataset=self.dataset,
            initial_count=self.initial_count,
            search_count=self.search_count,
            batch_size=self.batch_size,
            seed=self.seed,
        )
