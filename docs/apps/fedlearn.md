# Federated Learning

An implementation of a *Federated Learning* (FL) application.

At a high level, FL is a paradigm for performing deep learning on decentralized data hosted on decentralized devices (e.g., Internet-of-Things devices).
Each of these devices separately train their own copy of a shared model architecture.
Their locally-trained copies are aggregated over time to update a global model which has essentially been able to learn over all the data in the system without directly accessing any data.

Check out this [paper](http://proceedings.mlr.press/v54/mcmahan17a/mcmahan17a.pdf) to learn more about FL.

The `fedlearn` application uses simple deep neural networks and select set of baseline datasets to evaluate the task overheads and data costs associated with tasks in an FL app.

## Installation

This application requires numpy, PyTorch, and torchvision which can be installed automatically when installing the TaPS package.
```bash
pip install -e .[fedlearn]
```
If you want to use an accelerator for training (e.g., a GPU), you may need to follow specific instructions for the hardware or device driver versions.
Check out the [PyTorch docs](https://pytorch.org/) for more details.

## Example

The `fedlearn` application has many parameters, so we suggest taking a look at those available with `python -m taps.run fedlearn --help`.
A simple example can be run with:

```bash
python -m taps.run fedlearn --executor process-pool \
    --dataset mnist --data-dir data/fedlearn --rounds 1 --participation 0.5
```

The script will automatically download the training and testing data to `data/fedlearn`.

!!! warning

    CPU training can sometimes hang with certain executors (e.g., process pool).
    If this happens, try setting `OMP_NUM_THREADS=1`.

!!! warning

    Using the `fork` multiprocessing backend on MacOS can cause issues with
    PyTorch's backpropogation on CPU. This is most common with Parsl which
    forces the use of `fork`. If you encounter this issue, you may need
    to set the following.

    ```python
    import multiprocessing
    import platform

    if platform.system() == 'Darwin':
        multiprocessing.set_start_method('spawn', force=True)
    ```
