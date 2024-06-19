from __future__ import annotations

import abc
import pathlib
from typing import Protocol
from typing import runtime_checkable

from pydantic import BaseModel

from taps.engine import AppEngine


@runtime_checkable
class App(Protocol):
    """Application protocol."""

    def run(
        self,
        engine: AppEngine,
        run_dir: pathlib.Path,
    ) -> None:
        """Run the application."""
        ...

    def close(self) -> None:
        """Close the application."""
        ...


class AppConfig(BaseModel, abc.ABC):
    """Application config protocol.

    Application configs must define the `create_app()` method.
    """

    name: str

    @abc.abstractmethod
    def get_app(self) -> App:
        """Initialize an app instance from this config."""
        ...
