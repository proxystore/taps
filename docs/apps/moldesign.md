# Molecular Design

This application is based on [Molecular Design with Parsl](https://github.com/ExaWorks/molecular-design-parsl-demo/blob/6c0dbc598091634074cd3a8f23815819ab77f91e/0_molecular-design-with-parsl.ipynb).

## Installation

This application has certain dependencies which require Conda to install.
To get started, create a Conda environment, install the Conda dependencies, and then install the `taps` package into the Conda environment.

```bash
conda create -p $PWD/env python=3.11
conda activate ./env
conda install -c conda-forge xtb-python==22.1
pip install -e .[moldesign]
```

## Data

The data needs to be downloaded first.
```bash
curl -o data/QM9-search.tsv https://raw.githubusercontent.com/ExaWorks/molecular-design-parsl-demo/main/data/QM9-search.tsv
```

## Example

```bash
python -m taps.run moldesign --dataset data/QM9-search.tsv --executor process-pool --max-processes 4
```

Additional parameters are available with `python -m taps.run moldesign --help`.
It may be necessary to set `OMP_NUM_THREADS=1` with certain executors.
