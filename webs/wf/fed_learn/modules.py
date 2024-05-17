from __future__ import annotations

import pathlib

import torch
import torchvision
from torch import nn
from torch.nn import functional as F
from torch.utils.data import Dataset
from torchvision import transforms

from webs.wf.fed_learn.types import DataChoices


class CifarModule(nn.Module):
    """
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
        return self.network(x)


class MnistModule(nn.Module):
    """Model for MNIST and FashionMNIST data."""

    def __init__(self):
        super().__init__()
        self.flattener = nn.Flatten()
        self.fc1 = nn.Linear(28 * 28, 56 * 56)
        self.fc2 = nn.Linear(56 * 56, 28 * 28)
        self.fc3 = nn.Linear(28 * 28, 14 * 14)
        self.classifier = nn.Linear(14 * 14, 10)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.flattener(x)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = F.relu(self.fc3(x))
        x = self.classifier(x)
        return x


def create_model(data: DataChoices) -> nn.Module:
    """Initializes and returns a PyTorch model based on the name of the dataset in use.

    Args:
        data (DataChoices): Name of dataset that will be used for training (and testing).

    Raises:
        ValueError: Thrown if an illegal value for `DataChoices` is somehow passed in.

    Returns:
        PyTorch module to be used for FL workflow.

    Notes:
        The currently supported dataset options are `MNIST`, `FashionMNIST`, `CIFAR10`, and `CIFAR100`.
        We chose a conservative list of benchmark datasets for simplicity.
    """
    match data.value.lower():
        case "cifar10":
            return CifarModule(10)
        case "cifar100":
            return CifarModule(100)
        case "fmnist" | "mnist":
            return MnistModule()
        case _:
            raise ValueError(
                "Illegal value for function `load_model`. "
                "Supported values are 'cifar10', 'cifar100', 'fmnist', and 'mnist'."
            )


def load_data(
    data_name: DataChoices, root: pathlib.Path, train: bool, download: bool = False
) -> Dataset:
    """Load dataset to train with for FL workflow.

    Args:
        data_name (DataChoices): _description_
        root (pathlib.Path): _description_
        train (bool): _description_
        download (bool, optional): _description_. Defaults to False.

    Returns:
        Dataset: _description_
    """
    kwargs = dict(
        root=root, train=train, transform=transforms.ToTensor(), download=download
    )
    match data_name.value.lower():
        case "cifar10":
            return torchvision.datasets.CIFAR10(**kwargs)
        case "cifar100":
            return torchvision.datasets.CIFAR100(**kwargs)
        case "fmnist":
            return torchvision.datasets.FashionMNIST(**kwargs)
        case "mnist":
            return torchvision.datasets.MNIST(**kwargs)
        case _:
            raise ValueError("Illegal value for `load_data` function.")
