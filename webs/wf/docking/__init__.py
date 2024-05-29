"""Protein docking workflow.

The code in this module was adapted from:
[ParslDock](https://github.com/Parsl/parsl-docking-tutorial/blob/main/ParslDock.ipynb)

Protein docking aims to predict the orientation and
position of two molecules, where one molecule is a protein,
and the other is a protein or a smaller molecule (ligand).
Docking is commonly used in structure-based drug design, as
it can predict the strength of docking between target small molecule
ligands (drugs) to target binding site (receptors).

Need to install libGL and
[MGLTool](https://ccsb.scripps.edu/mgltools/download/491/mgltools_Linux-x86_64_1.5.7.tar.gz)
The environment variable MGLTOOLS_HOME needs to be set prior
to workflow execution. A Conda `environment.yaml` is provided within
the original notebook. It is recommended to install dependencies this way.
**NOTE**: Dependencies are not compatible with ARM64 architecture.

Sample input data provided in
[ParslDock tutorial](https://github.com/Parsl/parsl-docking-tutorial/blob/main/ParslDock.ipynb)
Input files needed include `dataset_orz_original_1k.csv` for
the SMILES string, `1iep_receptor.pdbqt` as the input receptor and
`set_element.tcl` as the tcl script path.

Example command:
```
python -m webs.run docking \
    --executor process-pool \
    --max-processes 40 \
    --smi-file-name-ligand ${PWD}/dataset_orz_original_1k.csv \
    --receptor ${PWD}/1iep_receptor.pdbqt \
    --tcl-path ${PWD}/set_element.tcl
```
"""

from __future__ import annotations
