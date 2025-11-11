from __future__ import annotations

from collections import OrderedDict

import torch
from numpy.random import Generator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from torch.utils.data import Dataset
from torch.utils.data import Subset

from taps.apps.fedlearn.modules import create_model
from taps.apps.fedlearn.types import DataChoices


class Client(BaseModel):
    """Client class."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    idx: int = Field(description='Client ID.')
    model: torch.nn.Module = Field(description='Client local model.')
    data: Subset | None = Field(
        description='Subset of data this client will train on.',
    )


def create_clients(
    num_clients: int,
    data_name: DataChoices,
    train: bool,
    train_data: Dataset,
    data_alpha: float,
    rng: Generator,
) -> list[Client]:
    """Create many clients with disjoint sets of data.

    Args:
        num_clients: Number of clients to create.
        data_name: The name of the data used. Used for initializing the
            corresponding model.
        train: If the application is using the no-op training task, then this
            function skips the step for giving each client their own subset
            of data.
        train_data: The original dataset that will be split across the clients.
        data_alpha: The
            [Dirichlet](https://en.wikipedia.org/wiki/Dirichlet_distribution)
            distribution alpha value for the number of samples across clients.
        rng: Random number generator.

    Returns:
        List of clients.
    """
    client_ids = list(range(num_clients))

    if train:
        client_indices: dict[int, list[int]] = {idx: [] for idx in client_ids}

        alpha = [data_alpha] * num_clients
        client_popularity = rng.dirichlet(alpha)

        for data_idx, _ in enumerate(train_data):
            client_id = rng.choice(client_ids, size=1, p=client_popularity)[0]
            client_indices[client_id].append(data_idx)

        client_subsets = {
            idx: Subset(train_data, client_indices[idx]) for idx in client_ids
        }
    else:
        client_subsets = dict.fromkeys(client_ids)

    clients = []
    for idx in client_ids:
        client = Client(
            idx=idx,
            model=create_model(data_name),
            data=client_subsets[idx],
        )
        clients.append(client)

    return clients


def unweighted_module_avg(
    selected_clients: list[Client],
) -> OrderedDict[str, torch.Tensor]:
    """Compute the unweighted average of models."""
    models = [client.model for client in selected_clients]
    w = 1 / len(selected_clients)

    with torch.no_grad():
        avg_weights = OrderedDict()
        for model in models:
            for name, value in model.state_dict().items():
                partial = w * torch.clone(value)
                if name not in avg_weights:
                    avg_weights[name] = partial
                else:
                    avg_weights[name] += partial

    return avg_weights
