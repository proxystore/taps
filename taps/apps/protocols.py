from __future__ import annotations

import pathlib
from typing import Protocol
from typing import runtime_checkable

from taps.executor.workflow import WorkflowExecutor


@runtime_checkable
class App(Protocol):
    """Application protocol."""

    def run(
        self,
        executor: WorkflowExecutor,
        run_dir: pathlib.Path,
    ) -> None:
        """Run the application."""
        ...

    def close(self) -> None:
        """Close the application."""
        ...
