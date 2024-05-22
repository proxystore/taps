from __future__ import annotations

import importlib
import pathlib
import sys
from types import TracebackType
from typing import Any
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


REGISTERED_WORKFLOWS = {
    'cholesky': 'webs.wf.cholesky.workflow.CholeskyWorkflow',
    'docking': 'webs.wf.docking.workflow.DockingWorkflow',
    'fedlearn': 'webs.wf.fedlearn.workflow.FedLearnWorkflow',
    'mapreduce': 'webs.wf.mapreduce.workflow.MapreduceWorkflow',
    'moldesign': 'webs.wf.moldesign.workflow.MoldesignWorkflow',
    'montage': 'webs.wf.montage.workflow.MontageWorkflow',
    'synthetic': 'webs.wf.synthetic.workflow.SyntheticWorkflow',
}


def get_registered_workflow_names() -> tuple[str, ...]:
    """Get the names of all registered workflows."""
    return tuple(REGISTERED_WORKFLOWS.keys())


def get_registered_workflow(name: str) -> type[Workflow[Any]]:
    """Get a workflow implementation by name.

    Args:
        name: Name of the registered workflow.

    Returns:
        The [`Workflow`][webs.workflow.Workflow] implementation type.

    Raises:
        KeyError: If `name` is not known.
        ImportError: If the workflow cannot be imported.
    """
    try:
        path = REGISTERED_WORKFLOWS[name]
    except KeyError as e:
        raise KeyError(
            f'A workflow named "{name}" has not been registered.',
        ) from e

    module_path, _, class_name = path.rpartition('.')

    try:
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(
            f'Failed to load the "{name}" workflow. (Tried to load '
            f'{class_name} from {module_path}.)\n\n'
            'If the above error is because another dependency was not found, '
            'check the documentation for the specific workflow '
            'for installation instructions or try reinstalling the package '
            'with the corresponding extras option.\n'
            f'  $ pip install .[{name}]',
        ) from e
