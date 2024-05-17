from __future__ import annotations

import pathlib
from enum import Enum

from pydantic import Field, model_validator
from typing_extensions import Self

from webs.config import Config
from webs.wf.fed_learn.types import DataChoices


class DataChoices(Enum):
    CIFAR10 = "cifar10"
    CIFAR100 = "cifar100"
    FMNIST = "fmnist"
    MNIST = "mnist"


class FedLearnWorkflowConfig(Config):
    """Config for the Federated Learning workflow."""

    data_alpha: float = Field(
        1e5, description="Alpha parameter for number of samples across clients."
    )
    """Dirichlet distribution parameter for tuning the uniformity of data across clients."""

    data_name: DataChoices = Field("mnist", description="The dataset to train on.")
    """Which data to use for training. The model is picked based on this parameter."""

    data_root: str | pathlib.Path = Field(
        pathlib.Path("./"), description="Location of where data are stored."
    )
    """Location of where the data are stored (or to be downloaded)."""

    data_download: bool = Field(
        False, description="Flag to set whether to download the data."
    )
    """Flag to set whether to download the data."""

    device: str = Field("cpu", description="Model fitting backend.")
    """Backend accelerator to perform model training."""

    num_clients: int = Field(100, description="The number of simulated clients.")
    """The number simulated clients."""

    num_rounds: int = Field(10, description="Number of aggregation rounds.")
    """The number of aggregation rounds."""

    participation: float = Field(
        0.1,
        description="The fraction of proportion of participating clients in each round.",
    )
    """The proportion of participating clients in each round."""

    #######################################################################
    # Hyperparameters related to local training on the simulated clients. #
    #######################################################################

    epochs: int = Field(3, description="Number of epochs for local training.")
    """Number of epochs for local training."""

    lr: float = Field(1e-3, description="Learning rate for local training on clients.")
    """Learning rate for local training on clients."""

    batch_size: int = Field(3, description="Batch size.")
    """Batch size while iterating through data."""

    train: bool = Field(True, description="Flag of whether to train or not.")
    """Flag of whether to train or not. If set to `False`, a no-op training job is used."""

    test: bool = Field(True, description="Flag of whether to test the global model.")
    """The global model will be tested (after aggregation) if set to `True`."""

    seed: int | None = Field(None, description="Random seed.")
    """Random seed for reproducibility."""
