from __future__ import annotations

import logging
import pathlib

import numpy
import torch

from taps.apps.fedlearn.client import create_clients
from taps.apps.fedlearn.client import unweighted_module_avg
from taps.apps.fedlearn.modules import create_model
from taps.apps.fedlearn.modules import load_data
from taps.apps.fedlearn.tasks import local_train
from taps.apps.fedlearn.tasks import no_local_train
from taps.apps.fedlearn.tasks import test_model
from taps.apps.fedlearn.types import DataChoices
from taps.apps.fedlearn.types import Result
from taps.engine import as_completed
from taps.engine import Engine
from taps.engine import TaskFuture
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)


class FedlearnApp:
    """Federated learning application.

    Args:
        clients: Number of simulated clients.
        rounds: Number of aggregation rounds to perform.
        dataset: Dataset (and corresponding model) to use.
        batch_size: Batch size used for local training across all clients.
        epochs: Number of epochs used during local training on all the clients.
        lr: Learning rate used during local training on all the clients.
        data_dir: Root directory where the dataset is stored or where
            you wish to download the data (i.e., `download=True`).
        device: Device to use for model training (e.g., `'cuda'`, `'cpu'`,
            `'mps'`).
        train: If `True` (default), the local training will be run. If `False,
            then a no-op version of the application will be performed where no
            training is done. This is useful for debugging purposes.
        test: If `True` (default), model testing is done at the end of each
            aggregation round.
        alpha: The number of data samples across clients is defined by a
            [Dirichlet](https://en.wikipedia.org/wiki/Dirichlet_distribution)
            distribution. This value is used to define the uniformity of the
            amount of data samples across all clients. When data alpha
            is large, then the number of data samples across
            clients is uniform (default). When the value is very small, then
            the sample distribution becomes more non-uniform. Note: this value
            must be greater than 0.
        participation: The portion of clients that participate in an
            aggregation round. If set to 1.0, then all clients participate in
            each round; if 0.5 then half of the clients, and so on. At least
            one client will be selected regardless of this value and the
            number of clients.
        seed: Seed for reproducibility.
    """

    def __init__(
        self,
        clients: int,
        rounds: int,
        dataset: DataChoices,
        batch_size: int,
        epochs: int,
        lr: float,
        data_dir: pathlib.Path,
        device: str = 'cpu',
        download: bool = False,
        train: bool = True,
        test: bool = True,
        alpha: float = 1e5,
        participation: float = 1.0,
        seed: int | None = None,
    ) -> None:
        self.rng = numpy.random.default_rng(seed)
        if seed is not None:
            torch.manual_seed(seed)

        self.dataset = dataset
        self.global_model = create_model(self.dataset)

        self.train, self.test = train, test
        self.train_data, self.test_data = None, None
        root = pathlib.Path(data_dir)
        if self.train:
            self.train_data = load_data(
                self.dataset,
                root,
                train=True,
                download=True,
            )
        if self.test:
            self.test_data = load_data(
                self.dataset,
                root,
                train=False,
                download=True,
            )

        self.device = torch.device(device)
        self.epochs = epochs
        self.batch_size = batch_size
        self.lr = lr

        self.participation = participation

        self.rounds = rounds
        if alpha <= 0:
            raise ValueError('Argument `alpha` must be greater than 0.')
        self.alpha = alpha

        self.clients = create_clients(
            clients,
            self.dataset,
            self.train,
            self.train_data,
            self.alpha,
            self.rng,
        )
        logger.log(APP_LOG_LEVEL, f'Created {len(self.clients)} clients')

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Directory for run outputs.
        """
        results = []
        for round_idx in range(self.rounds):
            preface = f'({round_idx+1}/{self.rounds})'
            logger.log(
                APP_LOG_LEVEL,
                f'{preface} Starting local training for this round',
            )

            train_result = self._federated_round(round_idx, engine, run_dir)
            results.extend(train_result)

            if self.test_data is not None:
                logger.log(
                    APP_LOG_LEVEL,
                    f'{preface} Starting the test for the global model',
                )
                test_result = engine.submit(
                    test_model,
                    self.global_model,
                    self.test_data,
                    round_idx,
                    self.device,
                ).result()
                logger.log(
                    APP_LOG_LEVEL,
                    f"{preface} Finished testing with test_loss="
                    f"{test_result['test_loss']:.3f}",
                )

    def _federated_round(
        self,
        round_idx: int,
        engine: Engine,
        run_dir: pathlib.Path,
    ) -> list[Result]:
        """Perform a single round in federated learning.

        Specifically, this method runs the following steps:

        1. client selection
        2. local training
        3. model aggregation

        Args:
            round_idx: Round number.
            engine: Application execution engine.
            run_dir: Run directory for results.

        Returns:
            List of results from each client.
        """
        job = local_train if self.train else no_local_train
        futures: list[TaskFuture[list[Result]]] = []
        results: list[Result] = []

        size = int(max(1, len(self.clients) * self.participation))
        assert 1 <= size <= len(self.clients)
        selected_clients = list(
            self.rng.choice(
                numpy.asarray(self.clients),
                size=size,
                replace=False,
            ),
        )

        for client in selected_clients:
            client.model.load_state_dict(self.global_model.state_dict())
            futures.append(
                engine.submit(
                    job,
                    client,
                    round_idx,
                    self.epochs,
                    self.batch_size,
                    self.lr,
                    self.device,
                ),
            )

        for fut in as_completed(futures):
            results.extend(fut.result())

        preface = f'({round_idx+1}/{self.rounds})'
        logger.log(APP_LOG_LEVEL, f'{preface} Finished local training')
        avg_params = unweighted_module_avg(selected_clients)
        self.global_model.load_state_dict(avg_params)
        logger.log(
            APP_LOG_LEVEL,
            f'{preface} Averaged the returned locally trained models',
        )

        return results
