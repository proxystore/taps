from __future__ import annotations

import json
import pathlib
import sys
from types import TracebackType
from typing import Any
from typing import Dict
from typing import Protocol

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


Record: TypeAlias = Dict[str, Any]
"""Record type."""


class RecordLogger(Protocol):
    """Record logger protocol."""

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None: ...

    def log(self, record: Record) -> None:
        """Log a record."""
        ...

    def close(self) -> None:
        """Close the logger."""
        ...


class JSONRecordLogger:
    """JSON lines record logger.

    Logs records as JSON strings per line to a file.

    Args:
        filepath: Filepath to log to.
    """

    def __init__(self, filepath: pathlib.Path | str) -> None:
        self._filepath = pathlib.Path(filepath)
        self._handle = open(self._filepath, 'a')  # noqa: SIM115

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def log(self, record: Record) -> None:
        """Log a record."""
        self._handle.write(json.dumps(record) + '\n')

    def close(self) -> None:
        """Close the logger."""
        self._handle.close()
