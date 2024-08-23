from __future__ import annotations

import pathlib
from typing import Literal

from pydantic import Field

from taps.apps import App
from taps.apps import AppConfig
from taps.plugins import register


@register('app')
class DockingConfig(AppConfig):
    """Docking application configuration."""

    name: Literal['docking'] = Field(
        'docking',
        description='Application name.',
    )
    smi_file_name_ligand: pathlib.Path = Field(
        description='Ligand SMILES string filepath.',
    )
    receptor: pathlib.Path = Field(
        description='Target receptor pdbqt filepath.',
    )
    tcl_path: pathlib.Path = Field(description='TCL script path.')
    initial_simulations: int = Field(
        8,
        description='Number of initial simulations.',
    )
    num_iterations: int = Field(
        3,
        description='Number of infer-simulate-train loops.',
    )
    batch_size: int = Field(
        8,
        description='Number of simulations per iteration.',
    )
    seed: int = Field(0, description='Random seed for sampling.')

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.docking.app import DockingApp

        return DockingApp(
            smi_file_name_ligand_path=pathlib.Path(self.smi_file_name_ligand),
            receptor_path=pathlib.Path(self.receptor),
            tcl_path=pathlib.Path(self.tcl_path),
            initial_simulations=self.initial_simulations,
            num_iterations=self.num_iterations,
            batch_size=self.batch_size,
            seed=self.seed,
        )
