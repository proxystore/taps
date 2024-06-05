# Molecular Design

This application is based on [Molecular Design with Parsl](https://github.com/ExaWorks/molecular-design-parsl-demo/blob/6c0dbc598091634074cd3a8f23815819ab77f91e/0_molecular-design-with-parsl.ipynb).

## Installation

This application has certain dependencies which require Conda to install.
To get started, create a Conda environment, install the Conda dependencies, and then install the `taps` package into the Conda environment.

```bash
conda create --name taps-moldesign python=3.11
conda activate taps-moldesign
conda install -c conda-forge xtb-python==22.1
pip install -e .[moldesign]
```

## Data

The data needs to be downloaded first.
```bash
curl -o data/moldesign/QM9-search.tsv --create-dirs https://raw.githubusercontent.com/ExaWorks/molecular-design-parsl-demo/main/data/QM9-search.tsv
```

## Example

The following configuration will execute tasks with a process pool of four workers.
An initial four simulations will be performed and the results of those simulations will train the initial model.
Then, the application will iteratively submit new batches of simulations for the highest ranked molecules from the model inference.
After the batch of simulations completes, the model is retrained and the cycle starts again until `search_count` molecules have been simulated.

```bash
python -m taps.run moldesign \
    --executor process-pool --max-processes 4 \
    --dataset data/moldesign/QM9-search.tsv \
    --initial-count 4 --batch-size 4 --search-count 16
```

Additional parameters are available with `python -m taps.run moldesign --help`.

!!! warning

    It may be necessary to set `OMP_NUM_THREADS=1` with certain executors if the simulation task appear to be stuck.

After the application completes, a CSV file containing the simulation results and a PNG graph of the molecules found over time will be saved to the run directory.
