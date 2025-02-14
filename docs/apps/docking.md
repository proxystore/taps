# Protein Docking

This application is modified from [ParslDock](https://github.com/Parsl/parsl-docking-tutorial/blob/main/ParslDock.ipynb).

Protein docking aims to predict the orientation and position of two molecules, where one molecule is a protein, and the other is a protein or a smaller molecule (ligand).
Docking is commonly used in structure-based drug design, as it can predict the strength of docking between target small molecule ligands (drugs) to target binding site (receptors).

## Installation

The docking application requires Conda to install all of the required dependencies.

```bash
conda create --name taps-docking python=3.11
conda activate taps-docking
conda install -c conda-forge -c bioconda autodock-vina libglu mgltools vmd
python3 -m pip install -e .[docking]
```

!!! warning

    This environment installs Python 2 and 3 so `python3` will need be be used for all commands.

The `MGLTOOLS_HOME` environment must be set.
If you install MGLTools with Conda, the path is likely the same as `CONDA_PREFIX`.
```
export MGLTOOLS_HOME=$CONDA_PREFIX
```

!!! warning

    Dependencies are not compatible with ARM64 architectures.

!!! tip

    Certain executors do not play nicely with the parallelism used by the simulation codes.
    If tasks appear to get suck, it may be necessary to set `OMP_NUM_THREADS=1`.

## Data

Sample input data is provided in the [ParslDock tutorial](https://github.com/Parsl/parsl-docking-tutorial/blob/main/ParslDock.ipynb).
The following CLI will download the `dataset_orz_original_1k.csv` (SMILES string), `1iep_receptor.pdbqt` (input receptor), and `set_element.tcl` (TCL script) files.

```bash
python3 -m taps.apps.docking.data --output data/docking/
```

## Example

The docking application can be invoked as follows.

```bash
python3 -m taps.run --app docking \
    --app.smi-file-name-ligand data/docking/dataset_orz_original_1k.csv \
    --app.receptor data/docking/1iep_receptor.pdbqt \
    --app.tcl-path data/docking/set_element.tcl
    --engine.executor process-pool \
    --engine.executor.max-processes 40 \
```

Checkout the full list of docking parameters with `python -m taps.run --app docking --help`.
For example, the `--app.batch-size` and `--app.num-iterations` parameters control the parallelism and length of the application.

!!! failure

    If you get an error that looks like the following:
    ```
    ImportError: /home/cc/miniconda3/envs/taps-docking/lib/python3.11/lib-dynload/_sqlite3.cpython-311-x86_64-linux-gnu.so: undefined symbol: sqlite3_trace_v2
    ```
    This is because Python is linking against a version of `libsqlite` different from what conda-forge packages were built against.
    Try installing sqlite from conda-forge directly:
    ```bash
    conda install conda-forge::sqlite conda-forge:libsqlite
    ```
    See [Issue #151](https://github.com/proxystore/taps/issues/151){target=_blank} for further discussion and debugging tips.
