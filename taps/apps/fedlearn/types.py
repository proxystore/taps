from __future__ import annotations

import enum
import sys
from typing import Any
from typing import Dict

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias


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
