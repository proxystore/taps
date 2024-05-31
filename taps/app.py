from __future__ import annotations

import abc
import pathlib
from typing import Protocol
from typing import runtime_checkable

from taps.config import Config
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


class AppConfig(abc.ABC, Config):
    """Application config protocol.

    Application configs inherit from [`Config`][taps.config.Config]
    and define the `create_app()` method.
    """

    @abc.abstractmethod
    def create_app(self) -> App:
        """Initialize an app instance from this config."""
        ...
