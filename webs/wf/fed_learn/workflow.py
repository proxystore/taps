from __future__ import annotations

import pathlib
import sys
import typing as t

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

# Workflow-specific imports for external libraries.
import numpy as np
import torch

from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor, as_completed
from webs.wf.fed_learn.config import FedLearnWorkflowConfig
from webs.wf.fed_learn.modules import create_model, load_data
from webs.wf.fed_learn.tasks import local_train, no_local_train, test_model
from webs.wf.fed_learn.utils import create_clients, log, unweighted_module_avg
from webs.workflow import register

if t.TYPE_CHECKING:
    from webs.executor.workflow import TaskFuture
    from webs.wf.fed_learn.types import DataChoices, Result


def _setup_platform():
    """
    On macOS, Parsl changes the default start method for multiprocessing to `fork`.
    This causes issues with backpropagation in PyTorch (i.e., `loss.backward()`), so
    this helper function forces the workflow to use `spawn`.
    """
    import platform

    if platform.system() == "Darwin":
        import multiprocessing as _multiprocessing

        _multiprocessing.set_start_method("spawn", force=True)


@register()
class FedLearnWorkflow(ContextManagerAddIn):

    name = "fed-learn"
    config_type = FedLearnWorkflowConfig

    def __init__(
        self,
        num_clients: int,
        num_rounds: int,
        data_name: DataChoices,
        batch_size: int,
        epochs: int,
        lr: float,
        root: str | pathlib.Path = "./",
        device: str = "cpu",
        download: bool = False,
        train: bool = True,
        test: bool = True,
        data_alpha: float = 1e5,
        participation_prob: float = 1.0,
        seed: int | None = None,
    ) -> None:
        """Initializes and sets up all the necessary components for the workflow.

        Args:
            num_clients (int): Number of simulated clients in the FL workflow.
            num_rounds (int): Number of aggregation rounds to perform.
            data_name (DataChoices): The data (and corresponding model) to use.
            batch_size (int): The batch size used for local training across all clients.
            epochs (int): The number of epochs used during local training on all the clients.
            lr (float): The learning rate used during local training on all the clients.
            root (str | pathlib.Path): The root directory where the dataset is stored or where
                you wish to download the data (i.e., `download=True`).
            device (torch.device): Which backend to use for model training (e.g., `'cuda'`, `'cpu'`, `'mps'`).
                This defaults to `'cpu'`.
            download (bool): If `True`, the dataset will be downloaded to the `root` arg directory.
                If `False` (default), the workflow will expect the data to already be downloaded.
            train (bool): If `True` (default), the local training (with PyTorch) will be run.
                If `False, then a no-op version of the workflow will be performed where no training is done.
                This is useful for debugging purposes.
            test (bool): If `True` (default), model testing is done at the end of each aggregation round.
            data_alpha (float): The number of data samples across clients is defined by a
                [Dirichlet](https://en.wikipedia.org/wiki/Dirichlet_distribution) distribution. This
                value is used to define the uniformity of the amount of data samples across all clients.
                When data alpha ($\\alpha$) is large, then the number of data samples across clients is
                uniform (default). When the value is very small, then the sample distribution becomes more
                non-uniform. Note: this value must be greater than 0.
            participation_prob (float): The portion of clients that participate in an aggregation round.
                If set to 1.0, then all clients participate in each round; if 0.5 then half of the clients,
                and so on. As a note, at least 1 client will be selected regardless of this value and the
                number of clients.
            seed (int | None): Seed for reproducibility.

        Notes:
            It is *suggested* that you download the data of interest ahead of running the workflow
            (i.e., set `download=False`).
        """
        self.rng = np.random.default_rng(seed)
        if seed is not None:
            torch.manual_seed(seed)

        self.data_name = data_name
        self.global_model = create_model(self.data_name)

        self.train, self.test = train, test
        self.train_data, self.test_data = None, None
        if self.train:
            self.train_data = load_data(
                self.data_name, root, train=True, download=download
            )
        if self.test:
            self.test_data = load_data(
                self.data_name, root, train=False, download=download
            )

        self.device = torch.device(device)
        self.epochs = epochs
        self.batch_size = batch_size
        self.lr = lr

        self.participation_prob = participation_prob

        self.num_rounds = num_rounds
        self.num_clients = num_clients
        if data_alpha <= 0:
            raise ValueError("Argument `data_alpha` must be greater than 0.")
        self.data_alpha = data_alpha

        log("Creating clients")
        self.clients = create_clients(
            self.num_clients,
            self.data_name,
            self.train,
            self.train_data,
            self.data_alpha,
            self.rng,
        )

        _setup_platform()

        super().__init__()

    @classmethod
    def from_config(cls, config: FedLearnWorkflowConfig) -> Self:
        """Load and initialize the workflow from a command line config.

        Args:
            config (FedLearnWorkflowConfig): Config instance.

        Returns:
            Instance of the workflow.
        """
        return cls(
            num_clients=config.num_clients,
            num_rounds=config.num_rounds,
            device=config.device,
            epochs=config.batch_size,
            batch_size=config.batch_size,
            lr=config.lr,
            data_name=config.data_name,
            root=config.data_root,
            download=config.data_download,
            train=config.train,
            test=config.test,
            participation_prob=config.participation,
            seed=config.seed,
        )

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Launches and runs the _Federated Learning_ (FL) workflow.

        Notes:
            For now, the collection of local training tasks is done sequentially. So,
            data-heavy clients/workers may act as a bottleneck until `as_completed`
            functionality is implemented.

        Args:
            executor (WorkflowExecutor): Executor to launch jobs/tasks for the workflow.
            run_dir (pathlib.Path): Directory for run outputs.
        """
        results = []
        for round_idx in range(self.num_rounds):
            preface = f"({round_idx+1}/{self.num_rounds})"
            log(f"{preface} Starting local training for this round.")

            res = self._federated_round(round_idx, executor, run_dir)
            results.extend(res)

            if self.test_data is not None:
                log(f"{preface} Starting the test for the global model.")
                fut = executor.submit(
                    test_model,
                    self.global_model,
                    self.test_data,
                    round_idx,
                    self.device,
                )
                res = fut.result()
                results.append(res)
                log(f"{preface} Finished testing with test_loss={res['test_loss']}")

        return results

    def _federated_round(
        self, round_idx: int, executor: WorkflowExecutor, run_dir: pathlib.Path
    ) -> list[Result]:
        """
        A single round in federated learning. Specifically, this method runs the following steps:
        1. client selection
        2. local training
        3. model aggregation

        Args:
            round_idx (int): Round number.
            executor (WorkflowExecutor): The executor to launch tasks with.
            run_dir (pathlib.Path): Run directory for results.
        """
        job = local_train if self.train else no_local_train
        futures: list[TaskFuture] = []
        results: list[Result] = []

        size = max(1, len(self.clients) * self.participation_prob)
        size = int(size)
        assert 1 <= size <= len(self.clients)
        selected_clients = self.rng.choice(self.clients, size=size, replace=False)

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
                )
            )

        for fut in as_completed(futures):
            results.extend(fut.result())

        preface = f"({round_idx+1}/{self.num_rounds})"
        log(f"{preface} Finished local training.")
        avg_params = unweighted_module_avg(selected_clients)
        self.global_model.load_state_dict(avg_params)
        log(f"{preface} Averaged the returned locally trained models.")

        return results
