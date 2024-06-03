from __future__ import annotations

import torch
from torch import nn
from torch.nn import functional as F  # noqa: N812
from torch.utils.data import DataLoader
from torch.utils.data import Dataset

from taps.apps.fedlearn.types import Client
from taps.apps.fedlearn.types import Result


def no_local_train(
    client: Client,
    round_idx: int,
    epochs: int,
    batch_size: int,
    lr: float,
    device: torch.device,
) -> list[Result]:
    """No-op version of [local_train][taps.apps.fedlearn.tasks.local_train].

    Returns:
        Empty result list.
    """
    return []


def local_train(
    client: Client,
    round_idx: int,
    epochs: int,
    batch_size: int,
    lr: float,
    device: torch.device,
) -> list[Result]:
    """Local training job.

    Args:
        client: The client to train.
        round_idx: The current round number.
        epochs: Number of epochs.
        batch_size: Batch size when iterating through data.
        lr: Learning rate.
        device: Backend hardware to train with.

    Returns:
        List of results that record the training history.
    """
    from datetime import datetime

    results: list[Result] = []
    client.model.to(device)
    client.model.train()
    optimizer = torch.optim.SGD(client.model.parameters(), lr=lr)
    loader = DataLoader(client.data, batch_size=batch_size)

    for epoch in range(epochs):
        epoch_results = []
        log_every_n_batches = 100
        running_loss = 0.0

        for batch_idx, batch in enumerate(loader):
            inputs, targets = batch
            inputs, targets = inputs.to(device), targets.to(device)
            preds = client.model(inputs)
            loss = F.cross_entropy(preds, targets)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            if batch_idx % log_every_n_batches == (log_every_n_batches - 1):
                epoch_results.append(
                    {
                        'time': datetime.now(),
                        'client_idx': client.idx,
                        'round_idx': round_idx,
                        'epoch': epoch,
                        'batch_idx': batch_idx,
                        'train_loss': running_loss / log_every_n_batches,
                    },
                )
                running_loss = 0.0

        results.extend(epoch_results)

    return results


def test_model(
    model: nn.Module,
    data: Dataset,
    round_idx: int,
    device: torch.device,
) -> Result:
    """Evaluate a model."""
    from datetime import datetime

    model.eval()
    with torch.no_grad():
        model.to(device)
        loader = DataLoader(data, batch_size=1)
        total_loss, n_batches = 0.0, 0
        for batch in loader:
            inputs, targets = batch
            inputs, targets = inputs.to(device), targets.to(device)
            preds = model(inputs)
            loss = F.cross_entropy(preds, targets)

            total_loss += loss.item()
            n_batches += 1

    res: Result = {
        'time': datetime.now(),
        'round_idx': round_idx,
        'test_loss': total_loss / n_batches,
    }
    return res
