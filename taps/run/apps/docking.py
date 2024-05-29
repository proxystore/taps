from __future__ import annotations

import pathlib

from pydantic import Field

from taps.apps.protocols import App
from taps.run.apps.registry import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='docking')
class DockingConfig(AppConfig):
    """Docking application configuration."""

    smi_file_name_ligand: str = Field(
        description='absolute path to ligand SMILES string',
    )
    receptor: str = Field(
        description='absolute path to target receptor pdbqt file',
    )
    tcl_path: str = Field(description='absolute path to TCL script')
    initial_simulations: int = Field(
        8,
        description='initial number of simulations to perform',
    )
    num_iterations: int = Field(
        3,
        description='number of infer-simulate-train loops to perform',
    )
    batch_size: int = Field(
        8,
        description='number of simulations per iteration',
    )
    seed: int = Field(0, description='random seed for sampling')

    def create_app(self) -> App:
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
