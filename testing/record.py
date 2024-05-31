from __future__ import annotations

import sys
from types import TracebackType
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from taps.record import Record


class SimpleRecordLogger:
    def __init__(self) -> None:
        self.records: list[Any] = []

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
        self.records.append(record)

    def close(self) -> None:
        return
