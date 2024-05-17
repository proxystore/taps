from __future__ import annotations

import typing as t
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field
from torch import nn
from torch.utils.data import Subset

ClientID: t.TypeAlias = int
"""Integer IDs for `Client` instances."""

Result: t.TypeAlias = dict[str, t.Any]
"""Result type for each FL epoch, round, and workflow."""


class Client(BaseModel):
    """Client class."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    idx: ClientID = Field(description="Client ID")
    """Client ID."""
    model: nn.Module = Field(description="Client's local model")
    """Client's local model."""
    data: Subset | None = Field(
        description="The subset of data this client will train on."
    )
    """The subset of data this client will train on."""


Client.model_rebuild()


class DataChoices(Enum):
    """Legal data options."""

    CIFAR10 = "cifar10"
    """CIFAR-10 dataset."""

    CIFAR100 = "cifar100"
    """CIFAR-100 dataset."""

    FMNIST = "fmnist"
    """FMNIST dataset."""

    MNIST = "mnist"
    """MNIST dataset."""
