from __future__ import annotations

from pydantic import Field

from webs.config import Config


class DockingWorkflowConfig(Config):
    """Synthetic workflow configuration."""

    output_dir: str = Field(description='Output directory to write results to')
    smi_file_name_ligand: str = Field(
        description='Path to ligand SMILES string',
    )
    receptor: str = Field(description='Path to target receptor pdbqt file')
    tcl_path: str = Field(description='Path to TCL script')
