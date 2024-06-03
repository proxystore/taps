from __future__ import annotations

import enum
import sys
from typing import Any
from typing import Dict
from typing import Optional

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from torch import nn
from torch.utils.data import Subset

ClientID: TypeAlias = int
"""Integer IDs for `Client` instances."""

Result: TypeAlias = Dict[str, Any]
"""Result type for each FL epoch, round, and task."""


class DataChoices(enum.Enum):
    """Dataset options."""

    CIFAR10 = 'cifar10'
    """Cifar10 dataset."""
    CIFAR100 = 'cifar100'
    """Cifar100 dataset."""
    FMNIST = 'fmnist'
    """FMNIST dataset."""
    MNIST = 'mnist'
    """MNIST dataset."""


class Client(BaseModel):
    """Client class."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    idx: ClientID = Field(description='Client ID')
    """Client ID."""
    model: nn.Module = Field(description="Client's local model")
    """Client's local model."""
    data: Optional[Subset] = Field(  # noqa: UP007
        description='The subset of data this client will train on.',
    )
    """The subset of data this client will train on."""
