from __future__ import annotations

import abc
import pathlib
import sys
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic import model_validator

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

    After instantiation, all [`pathlib.Path`][pathlib.Path] types will
    be resolved to convert them to absolute paths.
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

    @field_serializer('*')
    def _serialize_path_as_str(self, field: Any) -> Any:
        if isinstance(field, pathlib.Path):
            return str(field)
        return field

    @model_validator(mode='after')
    def _resolve_path_types(self) -> Self:
        for name, value in self:
            if isinstance(value, pathlib.Path):
                setattr(self, name, value.resolve())
        return self
