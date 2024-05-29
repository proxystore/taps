from __future__ import annotations

from pydantic import Field

from taps.config import Config


class DockingWorkflowConfig(Config):
    """Synthetic workflow configuration."""

    smi_file_name_ligand: str = Field(
        description='Absolute path to ligand SMILES string',
    )
    receptor: str = Field(
        description='Absolute path to target receptor pdbqt file',
    )
    tcl_path: str = Field(description='Absolute path to TCL script')
    initial_simulations: int = Field(
        8,
        description='Initial number of simulations to perform',
    )
    num_iterations: int = Field(
        3,
        description='Number of infer-simulate-train loops to perform',
    )
    batch_size: int = Field(
        8,
        description='Number of simulations per iteration',
    )
    seed: int = Field(0, description='Random seed for sampling')
