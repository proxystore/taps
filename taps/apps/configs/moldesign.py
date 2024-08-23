from __future__ import annotations

import pathlib
from typing import Literal

from pydantic import Field

from taps.apps import App
from taps.apps import AppConfig
from taps.plugins import register


@register('app')
class MoldesignConfig(AppConfig):
    """Moldesign application configuration."""

    name: Literal['moldesign'] = Field(
        'moldesign',
        description='Application name.',
    )
    dataset: pathlib.Path = Field(description='Molecule search space dataset.')
    initial_count: int = Field(
        8,
        description='Number of initial calculations.',
    )
    search_count: int = Field(
        64,
        description='Number of molecules to evaluate in total.',
    )
    batch_size: int = Field(
        4,
        description=(
            'Number of molecules to evaluate in each batch of simulations.'
        ),
    )
    seed: int = Field(0, description='Random seed.')

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
