from __future__ import annotations

import pathlib
import sys
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel

from webs.executor.workflow import WorkflowExecutor

WorkflowConfigT = TypeVar('WorkflowConfigT', bound=BaseModel)


@runtime_checkable
class Workflow(Protocol[WorkflowConfigT]):
    """Workflow protocol.

    Attributes:
        name: Name of the workflow.
        config_type: Workflow configuration type.
    """

    name: str
    config_type: type[WorkflowConfigT]

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None: ...

    @classmethod
    def from_config(cls, config: WorkflowConfigT) -> Self:
        """Initialize a workflow instance from a config."""
        ...

    def run(
        self,
        executor: WorkflowExecutor,
        run_dir: pathlib.Path,
    ) -> None:
        """Run the workflow."""
        ...


class _WorkflowRegistry:
    def __init__(self) -> None:
        self._workflows: dict[str, type[Workflow[Any]]] = {}

    def get_registered(self) -> dict[str, type[Workflow[Any]]]:
        return self._workflows

    def register(
        self,
        *,
        name: str | None = None,
    ) -> Callable[[type[Workflow[Any]]], type[Workflow[Any]]]:
        def decorator(cls: type[Workflow[Any]]) -> type[Workflow[Any]]:
            registered_name = (
                cls.name.lower().replace(' ', '-').replace('_', '-')
                if name is None
                else name
            )
            self._workflows[registered_name] = cls
            return cls

        return decorator


_workflows = _WorkflowRegistry()
register = _workflows.register
get_registered = _workflows.get_registered
