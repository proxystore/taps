from __future__ import annotations

import pathlib
from typing import Literal
from typing import Optional

from pydantic import Field
from pydantic import field_validator

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='fedlearn')
class FedlearnConfig(AppConfig):
    """Federated learning application configuration."""

    name: Literal['fedlearn'] = 'fedlearn'
    alpha: float = Field(
        1e5,
        description='alpha parameter for number of samples across clients',
    )
    dataset: str = Field(
        'mnist',
        description=(
            'training and testing dataset (cifar10, cifar100, fmnist, mnist)'
        ),
    )
    data_dir: str = Field(
        'data/fedlearn',
        description='download directory for data',
    )
    device: str = Field('cpu', description='device to use (e.g., cpu or cuda)')
    clients: int = Field(
        8,
        description='number of simulated clients',
    )
    rounds: int = Field(3, description='number of aggregation rounds')
    participation: float = Field(
        0.1,
        description='fraction of participating clients in each round',
    )
    epochs: int = Field(3, description='number of epochs for local training')
    lr: float = Field(
        1e-3,
        description='learning rate for local training',
    )
    batch_size: int = Field(3, description='batch size for local training')
    train: bool = Field(
        True,
        description=(
            'flag for performing training '
            '(false will use no-op training tasks)'
        ),
    )
    test: bool = Field(
        True,
        description='evaluate the global model on test data after each round',
    )
    seed: Optional[int] = Field(None, description='random seed')  # noqa: UP007

    @field_validator('data_dir', mode='before')
    @classmethod
    def _resolve_data_root(cls, root: str) -> str:
        return str(pathlib.Path(root).resolve())

    @field_validator('dataset', mode='after')
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

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.fedlearn.app import FedlearnApp
        from taps.apps.fedlearn.types import DataChoices

        return FedlearnApp(
            clients=self.clients,
            rounds=self.rounds,
            device=self.device,
            epochs=self.batch_size,
            batch_size=self.batch_size,
            lr=self.lr,
            dataset=DataChoices(self.dataset),
            data_dir=self.data_dir,
            train=self.train,
            test=self.test,
            participation=self.participation,
            seed=self.seed,
        )
