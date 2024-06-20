from __future__ import annotations

import abc
from concurrent.futures import Executor

from pydantic import BaseModel


class ExecutorConfig(BaseModel, abc.ABC):
    """Abstract executor configuration."""

    name: str

    @abc.abstractmethod
    def get_executor(self) -> Executor:
        """Create an executor from the configuration."""
        ...
