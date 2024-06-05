from __future__ import annotations

import pathlib

from pydantic import Field
from pydantic import field_validator

from taps.app import App
from taps.app import AppConfig
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

    @field_validator(
        'smi_file_name_ligand',
        'receptor',
        'tcl_path',
        mode='before',
    )
    @classmethod
    def _resolve_filepaths(cls, path: str) -> str:
        return str(pathlib.Path(path).resolve())

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
