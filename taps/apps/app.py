from __future__ import annotations

import abc
import pathlib
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING

from pydantic import BaseModel
from pydantic import ConfigDict

if TYPE_CHECKING:
    from taps.engine import Engine


@runtime_checkable
class App(Protocol):
    """Application protocol."""

    def run(
        self,
        engine: Engine,
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

    model_config: ConfigDict = ConfigDict(  # type: ignore[misc]
        extra='forbid',
        validate_default=True,
        validate_return=True,
    )

    @abc.abstractmethod
    def get_app(self) -> App:
        """Initialize an app instance from this config."""
        ...
