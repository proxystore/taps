from __future__ import annotations

import logging
import multiprocessing
import pathlib
import platform
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

import numpy as np
import torch

from taps.executor.workflow import as_completed
from taps.executor.workflow import TaskFuture
from taps.executor.workflow import WorkflowExecutor
from taps.logging import WORK_LOG_LEVEL
from taps.wf.fedlearn.config import DataChoices
from taps.wf.fedlearn.modules import create_model
from taps.wf.fedlearn.modules import load_data
from taps.wf.fedlearn.tasks import local_train
from taps.wf.fedlearn.tasks import no_local_train
from taps.wf.fedlearn.tasks import test_model
from taps.wf.fedlearn.types import Result
from taps.wf.fedlearn.utils import create_clients
from taps.wf.fedlearn.utils import unweighted_module_avg

logger = logging.getLogger(__name__)


def _setup_platform() -> None:
    # On macOS, Parsl changes the default start method for
    # multiprocessing to `fork`. This causes issues with backpropagation
    # in PyTorch (i.e., `loss.backward()`), so this helper function
    # forces the workflow to use `spawn`.
    if platform.system() == 'Darwin':
        multiprocessing.set_start_method('spawn', force=True)


class FedlearnApp:
    """Federated learning application.

    Tip:
        Download the data of interest ahead of running the workflow
        (i.e., set `download=False`).

    Args:
        num_clients: Number of simulated clients in the FL workflow.
        num_rounds: Number of aggregation rounds to perform.
        data_name: Data (and corresponding model) to use.
        batch_size: Batch size used for local training across all clients.
        epochs: Number of epochs used during local training on all the clients.
        lr: Learning rate used during local training on all the clients.
        data_dir: Root directory where the dataset is stored or where
            you wish to download the data (i.e., `download=True`).
        device: Device to use for model training (e.g., `'cuda'`, `'cpu'`,
            `'mps'`).
        download: If `True`, the dataset will be downloaded to the `root`
            arg directory. If `False` (default), the workflow will expect the
            data to already be downloaded.
        train: If `True` (default), the local training will be run. If `False,
            then a no-op version of the workflow will be performed where no
            training is done. This is useful for debugging purposes.
        test: If `True` (default), model testing is done at the end of each
            aggregation round.
        data_alpha: The number of data samples across clients is defined by a
            [Dirichlet](https://en.wikipedia.org/wiki/Dirichlet_distribution)
            distribution. This value is used to define the uniformity of the
            amount of data samples across all clients. When data alpha
            is large, then the number of data samples across
            clients is uniform (default). When the value is very small, then
            the sample distribution becomes more non-uniform. Note: this value
            must be greater than 0.
        participation_prob: The portion of clients that participate in an
            aggregation round. If set to 1.0, then all clients participate in
            each round; if 0.5 then half of the clients, and so on. At least
            one client will be selected regardless of this value and the
            number of clients.
        seed: Seed for reproducibility.
    """

    def __init__(
        self,
        num_clients: int,
        num_rounds: int,
        data_name: DataChoices,
        batch_size: int,
        epochs: int,
        lr: float,
        data_dir: str = 'data/',
        device: str = 'cpu',
        download: bool = False,
        train: bool = True,
        test: bool = True,
        data_alpha: float = 1e5,
        participation_prob: float = 1.0,
        seed: int | None = None,
    ) -> None:
        self.rng = np.random.default_rng(seed)
        if seed is not None:
            torch.manual_seed(seed)

        self.data_name = data_name
        self.global_model = create_model(self.data_name)

        self.train, self.test = train, test
        self.train_data, self.test_data = None, None
        root = pathlib.Path(data_dir)
        if self.train:
            self.train_data = load_data(
                self.data_name,
                root,
                train=True,
                download=download,
            )
        if self.test:
            self.test_data = load_data(
                self.data_name,
                root,
                train=False,
                download=download,
            )

        self.device = torch.device(device)
        self.epochs = epochs
        self.batch_size = batch_size
        self.lr = lr

        self.participation_prob = participation_prob

        self.num_rounds = num_rounds
        self.num_clients = num_clients
        if data_alpha <= 0:
            raise ValueError('Argument `data_alpha` must be greater than 0.')
        self.data_alpha = data_alpha

        logger.log(WORK_LOG_LEVEL, 'Creating clients')
        self.clients = create_clients(
            self.num_clients,
            self.data_name,
            self.train,
            self.train_data,
            self.data_alpha,
            self.rng,
        )

        _setup_platform()

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Executor to launch jobs/tasks for the workflow.
            run_dir: Directory for run outputs.
        """
        results = []
        for round_idx in range(self.num_rounds):
            preface = f'({round_idx+1}/{self.num_rounds})'
            logger.log(
                WORK_LOG_LEVEL,
                f'{preface} Starting local training for this round.',
            )

            train_result = self._federated_round(round_idx, executor, run_dir)
            results.extend(train_result)

            if self.test_data is not None:
                logger.log(
                    WORK_LOG_LEVEL,
                    f'{preface} Starting the test for the global model.',
                )
                test_result = executor.submit(
                    test_model,
                    self.global_model,
                    self.test_data,
                    round_idx,
                    self.device,
                ).result()
                logger.log(
                    WORK_LOG_LEVEL,
                    f"{preface} Finished testing with test_loss="
                    f"{test_result['test_loss']}",
                )

    def _federated_round(
        self,
        round_idx: int,
        executor: WorkflowExecutor,
        run_dir: pathlib.Path,
    ) -> list[Result]:
        """Perform a single round in federated learning.

        Specifically, this method runs the following steps:

        1. client selection
        2. local training
        3. model aggregation

        Args:
            round_idx: Round number.
            executor: The executor to launch tasks with.
            run_dir: Run directory for results.
        """
        job = local_train if self.train else no_local_train
        futures: list[TaskFuture[list[Result]]] = []
        results: list[Result] = []

        size = max(1, len(self.clients) * self.participation_prob)
        size = int(size)
        assert 1 <= size <= len(self.clients)
        selected_clients = self.rng.choice(
            self.clients,
            size=size,
            replace=False,
        )

        for client in selected_clients:
            client.model.load_state_dict(self.global_model.state_dict())
            futures.append(
                executor.submit(
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

        preface = f'({round_idx+1}/{self.num_rounds})'
        logger.log(WORK_LOG_LEVEL, f'{preface} Finished local training.')
        avg_params = unweighted_module_avg(selected_clients)
        self.global_model.load_state_dict(avg_params)
        logger.log(
            WORK_LOG_LEVEL,
            f'{preface} Averaged the returned locally trained models.',
        )

        return results
