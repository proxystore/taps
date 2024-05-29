from __future__ import annotations

import sys
from types import TracebackType

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from taps.context import ContextManagerAddIn


class _TestContextManager:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    def __enter__(self) -> Self:
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.exited = True


def test_context_manager_add_in() -> None:
    class _TestClass(ContextManagerAddIn):
        def __init__(self, manager: _TestContextManager) -> None:
            self.closed = False
            super().__init__(managers=[manager, None])

        def close(self) -> None:
            self.closed = True

    manager = _TestContextManager()
    with _TestClass(manager) as test_class:
        pass

    assert test_class.closed
    assert manager.entered
    assert manager.exited
