from __future__ import annotations

import logging
import typing as t
from collections import OrderedDict
from datetime import datetime

import torch
from numpy.random import Generator
from torch.nn import functional as F
from torch.optim import Optimizer
from torch.utils.data import DataLoader, Subset

from webs.logging import WORK_LOG_LEVEL
from webs.wf.fed_learn.modules import create_model
from webs.wf.fed_learn.types import Client, ClientID, DataChoices, Result

if t.TYPE_CHECKING:
    from torch.utils.data import Dataset

work_logger = logging.getLogger(__name__)


def create_clients(
    num_clients: int,
    data_name: DataChoices,
    train: bool,
    train_data: Dataset,
    data_alpha: float,
    rng: Generator,
) -> list[Client]:
    """
    A utility function for creating a number of clients with disjoint sets of data.

    Args:
        num_clients (int):
        data_name (DataChoices): The name of the data used. This is used for initializing the
            corresponding model.
        train (bool): If the workflow is using the no-op training task, then this function
            skips the step for giving each client their own subset of data.
        train_data (Dataset): The original dataset that will be split across the clients.
        data_alpha (float): The [Dirichlet](https://en.wikipedia.org/wiki/Dirichlet_distribution)
            distribution alpha value for the number of samples across clients.
        rng (Generator): Random number generator.

    Returns:
        List of clients for the workflow.
    """
    client_ids = list(range(num_clients))

    if train:
        client_indices = {idx: [] for idx in client_ids}

        alpha = [data_alpha] * num_clients
        client_popularity = rng.dirichlet(alpha)

        for data_idx, _ in enumerate(train_data):
            selected_client: ClientID = rng.choice(
                client_ids, size=1, p=client_popularity
            )
            selected_client = selected_client[0]
            client_indices[selected_client].append(data_idx)

        client_subsets = {
            idx: Subset(train_data, client_indices[idx]) for idx in client_ids
        }
    else:
        client_subsets = {idx: None for idx in client_ids}

    clients = []
    for idx in client_ids:
        client = Client(
            idx=idx, model=create_model(data_name), data=client_subsets[idx]
        )
        clients.append(client)

    return clients


def log(msg: str) -> None:
    work_logger.log(WORK_LOG_LEVEL, msg)


def unweighted_module_avg(selected_clients: list[Client]) -> OrderedDict:
    models = [client.model for client in selected_clients]
    w = 1 / len(selected_clients)

    with torch.no_grad():
        avg_weights = OrderedDict()
        for model in models:
            for name, value in model.state_dict().items():
                value = w * torch.clone(value)
                if name not in avg_weights:
                    avg_weights[name] = value
                else:
                    avg_weights[name] += value

    return avg_weights
