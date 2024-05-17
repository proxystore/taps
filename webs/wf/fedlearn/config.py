from __future__ import annotations

import pathlib
from enum import Enum
from typing import Optional

from pydantic import Field
from pydantic import field_validator

from webs.config import Config


class DataChoices(Enum):
    """Dataset options."""

    CIFAR10 = 'cifar10'
    CIFAR100 = 'cifar100'
    FMNIST = 'fmnist'
    MNIST = 'mnist'


class FedLearnWorkflowConfig(Config):
    """Config for the Federated Learning workflow."""

    data_alpha: float = Field(
        1e5,
        description='alpha parameter for number of samples across clients',
    )
    data_name: DataChoices = Field(
        'mnist',  # type: ignore[assignment]
        description='the dataset to train on',
    )
    data_root: str = Field(
        'data/',
        description='location of where data are stored',
    )
    data_download: bool = Field(
        False,
        description='flag to set whether to download the data',
    )
    device: str = Field('cpu', description='model fitting backend')
    num_clients: int = Field(
        8,
        description='the number of simulated clients',
    )
    num_rounds: int = Field(3, description='number of aggregation rounds')
    participation: float = Field(
        0.1,
        description=(
            'fraction of proportion of participating clients in each round'
        ),
    )
    epochs: int = Field(3, description='number of epochs for local training')
    lr: float = Field(
        1e-3,
        description='learning rate for local training on clients',
    )
    batch_size: int = Field(3, description='batch size')
    train: bool = Field(True, description='flag of whether to train or not')
    test: bool = Field(
        True,
        description='flag of whether to test the global model',
    )
    seed: Optional[int] = Field(None, description='random seed')  # noqa: UP007

    @field_validator('data_root', mode='before')
    @classmethod
    def _resolve_data_root(cls, root: str) -> str:
        return str(pathlib.Path(root).resolve())
