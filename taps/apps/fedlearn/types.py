from __future__ import annotations

import enum
from typing import Any
from typing import TypeAlias

Result: TypeAlias = dict[str, Any]
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
