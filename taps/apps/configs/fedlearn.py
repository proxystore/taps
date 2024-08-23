from __future__ import annotations

import pathlib
from typing import Literal
from typing import Optional

from pydantic import Field
from pydantic import field_validator

from taps.apps import App
from taps.apps import AppConfig
from taps.apps.fedlearn.types import DataChoices
from taps.plugins import register


@register('app')
class FedlearnConfig(AppConfig, use_enum_values=True):
    """Federated learning application configuration."""

    name: Literal['fedlearn'] = Field(
        'fedlearn',
        description='Application name.',
    )
    alpha: float = Field(
        1e5,
        description='Alpha parameter for number of samples across clients.',
    )
    dataset: DataChoices = Field(
        DataChoices.MNIST,
        description='Training and testing dataset.',
    )
    data_dir: pathlib.Path = Field(
        pathlib.Path('data/fedlearn'),
        description='Dataset download directory.',
    )
    device: str = Field(
        'cpu',
        description='Device to use (e.g., cpu or cuda).',
    )
    clients: int = Field(
        8,
        description='Number of simulated clients.',
    )
    rounds: int = Field(3, description='Number of aggregation rounds.')
    participation: float = Field(
        1.0,
        description='Fraction of participating clients in each round.',
    )
    epochs: int = Field(3, description='Number of epochs for local training.')
    lr: float = Field(
        1e-3,
        description='Learning rate for local training.',
    )
    batch_size: int = Field(3, description='Batch size for local training.')
    train: bool = Field(
        True,
        description=(
            'Flag for performing training '
            '(false will use no-op training tasks).'
        ),
    )
    test: bool = Field(
        True,
        description='Evaluate the global model on test data after each round.',
    )
    seed: Optional[int] = Field(None, description='Random seed.')  # noqa: UP007

    @field_validator('dataset', mode='before')
    @classmethod
    def _validate_dataset(cls, value: str) -> str:
        return value.lower()

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
