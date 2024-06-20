from __future__ import annotations

import abc
from concurrent.futures import Executor

from pydantic import BaseModel


class ExecutorConfig(BaseModel, abc.ABC):
    """Executor configuration."""

    name: str

    @abc.abstractmethod
    def get_executor(self) -> Executor: ...
