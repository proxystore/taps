from __future__ import annotations

from pydantic import Field

from webs.config import Config


class DockingWorkflowConfig(Config):
    """Synthetic workflow configuration."""

    smi_file_name_ligand: str = Field(
        description='Absolute path to ligand SMILES string',
    )
    receptor: str = Field(
        description='Absolute path to target receptor pdbqt file',
    )
    tcl_path: str = Field(description='Absolute path to TCL script')
