from __future__ import annotations

from typing import Callable
from typing import Protocol
from typing import runtime_checkable


@runtime_checkable
class Workflow(Protocol):
    name: str


class _WorkflowRegistry:
    def __init__(self) -> None:
        self._workflows: dict[str, type[Workflow]] = {}

    def get_registered(self) -> dict[str, type[Workflow]]:
        return self._workflows

    def register(
        self,
        *,
        name: str,
    ) -> Callable[[type[Workflow]], type[Workflow]]:
        def decorator(cls: type[Workflow]) -> type[Workflow]:
            self._workflows[name] = cls
            return cls

        return decorator


workflows = _WorkflowRegistry()
register = workflows.register
get_registered = workflows.get_registered
