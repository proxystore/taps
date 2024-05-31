from __future__ import annotations

import pathlib
from typing import Optional

from pydantic import Field
from pydantic import field_validator

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='fedlearn')
class FedlearnConfig(AppConfig):
    """Federated learning application configuration."""

    data_alpha: float = Field(
        1e5,
        description='alpha parameter for number of samples across clients',
    )
    data_name: str = Field(
        'mnist',
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

    @field_validator('data_name', mode='after')
    @classmethod
    def _validate_dataset(cls, dataset: str) -> str:
        from taps.apps.fedlearn.types import DataChoices

        try:
            DataChoices(dataset)
        except KeyError:
            options = ', '.join(d.value for d in DataChoices)
            raise ValueError(
                f'{dataset} is not a supported dataset. '
                f'Must be one of {options}.',
            ) from None

        return dataset

    def create_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.fedlearn.app import FedlearnApp
        from taps.apps.fedlearn.types import DataChoices

        return FedlearnApp(
            num_clients=self.num_clients,
            num_rounds=self.num_rounds,
            device=self.device,
            epochs=self.batch_size,
            batch_size=self.batch_size,
            lr=self.lr,
            data_name=DataChoices(self.data_name),
            data_dir=self.data_root,
            download=self.data_download,
            train=self.train,
            test=self.test,
            participation_prob=self.participation,
            seed=self.seed,
        )
