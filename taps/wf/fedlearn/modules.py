from __future__ import annotations

import pathlib

import torch
import torchvision
from torch import nn
from torch.nn import functional as F  # noqa: N812
from torch.utils.data import Dataset
from torchvision import transforms

from taps.wf.fedlearn.config import DataChoices


class CifarModule(nn.Module):
    """Cifar model.

    Source:
    https://www.kaggle.com/code/shadabhussain/cifar-10-cnn-using-pytorch
    """

    def __init__(self, num_classes: int):
        super().__init__()
        self.num_classes = num_classes
        self.network = nn.Sequential(
            nn.Conv2d(3, 32, kernel_size=3, padding=1),
            nn.ReLU(),
            nn.Conv2d(32, 64, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2, 2),  # output: 64 x 16 x 16
            nn.Conv2d(64, 128, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.Conv2d(128, 128, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2, 2),  # output: 128 x 8 x 8
            nn.Conv2d(128, 256, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.Conv2d(256, 256, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2, 2),  # output: 256 x 4 x 4
            nn.Flatten(),
            nn.Linear(256 * 4 * 4, 1024),
            nn.ReLU(),
            nn.Linear(1024, 512),
            nn.ReLU(),
            nn.Linear(512, num_classes),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass."""
        return self.network(x)


class MnistModule(nn.Module):
    """Model for MNIST and FashionMNIST data."""

    def __init__(self) -> None:
        super().__init__()
        self.flattener = nn.Flatten()
        self.fc1 = nn.Linear(28 * 28, 56 * 56)
        self.fc2 = nn.Linear(56 * 56, 28 * 28)
        self.fc3 = nn.Linear(28 * 28, 14 * 14)
        self.classifier = nn.Linear(14 * 14, 10)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass."""
        x = self.flattener(x)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = F.relu(self.fc3(x))
        x = self.classifier(x)
        return x


def create_model(data: DataChoices) -> nn.Module:
    """Create a model suitable for the dataset choice.

    Note:
        The currently supported dataset options are `MNIST`, `FashionMNIST`,
        `CIFAR10`, and `CIFAR100`.

    Args:
        data: Name of dataset that will be used for training (and testing).

    Returns:
        PyTorch module to be used for FL workflow.

    Raises:
        ValueError: If an unsupported value for `data` is provided.
    """
    name = data.value.lower()

    if name == 'cifar10':
        return CifarModule(10)
    elif name == 'cifar100':
        return CifarModule(100)
    elif name in ('fmnist', 'mnist'):
        return MnistModule()
    else:
        raise ValueError(
            f'Unknown dataset "{data.value}". Supported options are '
            "'cifar10', 'cifar100', 'fmnist', and 'mnist'.",
        )


def load_data(
    data_name: DataChoices,
    root: pathlib.Path,
    train: bool,
    download: bool = False,
) -> Dataset:
    """Load dataset to train with for FL workflow.

    Args:
        data_name: Dataset choice.
        root: Root dataset directory.
        train: Flag for if training.
        download: Should the dataset be downloaded.

    Returns:
        Dataset: _description_
    """
    kwargs = {
        'root': root,
        'train': train,
        'transform': transforms.ToTensor(),
        'download': download,
    }
    name = data_name.value.lower()
    if name == 'cifar10':
        return torchvision.datasets.CIFAR10(**kwargs)
    elif name == 'cifar100':
        return torchvision.datasets.CIFAR100(**kwargs)
    elif name == 'fmnist':
        return torchvision.datasets.FashionMNIST(**kwargs)
    elif name == 'mnist':
        return torchvision.datasets.MNIST(**kwargs)
    else:
        raise ValueError(f'Unknown dataset: {data_name}.')
