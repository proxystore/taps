from __future__ import annotations

import pathlib

from pydantic import Field
from pydantic import field_validator

from webs.config import Config


class MoldesignWorkflowConfig(Config):
    """Moldesign workflow configuration."""

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
