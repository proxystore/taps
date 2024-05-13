# Creating Workflows

This guide describes creating a workflow within the WEBS framework.

## Installation

A development environment needs to be configured first.
Fork the repository and clone your fork locally.
Then, configure a virtual environment with the WEBS package and development dependencies.

```bash
python -m venv venv
. venv/bin/activate
pip install -e .[dev,docs]
```

See [Getting Started for Local Development](../contributing/index.md#getting-started-for-local-development) for detailed instructions on running the linters and continuous integration tests.

## Workflow Structure

Our example workflow is going to be called `foobar`.
All workflows in WEBS are composed of two required components:
a [`Config`][webs.config.Config] and a [`Workflow`][webs.workflow.Workflow].
The [`Config`][webs.config.Config] is a Pydantic [`BaseModel`][pydantic.BaseModel] with some extra functionality for constructing a config instance from command line arguments.
The [`Workflow`][webs.workflow.Workflow] is a protocol with two key methods to implement:
[`from_config()`][webs.workflow.Workflow.from_config] and [`run()`][webs.workflow.Workflow.run].

All workflows are submodules of `webs/wf/`.
In this example, we will create the `webs/wf/foobar` directory containing the following files.

```
webs/
├─ wf/
│  ├─ __init__.py
│  ├─ foobar/
│  │  ├─ __init__.py
│  │  ├─ config.py
│  │  ├─ workflow.py
```

The first file, `webs/wf/foobar/__init__.py`, will contain the following lines.
```python title="webs/wf/foobar/__init__.py" linenums="1"
from __future__ import annotations

import webs.wf.foobar.workflow
```
This import is necessary to run some registration code we will add in `webs/wf/foobar/workflow.py`.
We will also need to add the following line to `webs/wf/__init__.py` or the above import will not be run when the CLI is invoked.
```python
import webs.wf.foobar
```

The second file, `webs/wf/foobar/config.py`, will contain the configuration model for the workflow.
This configuration should contain all of the parameters that the user needs to provide or that can be adjusted for the workflow.
In our case, the `foobar` workflow is simply going to print a user defined message `n` number of times.
```python title="webs/wf/foobar/config.py" linenums="1"
from __future__ import annotations

from pydantic import Field

from webs.config import Config


class FoobarWorkflowConfig(Config):
    """Foobar workflow configuration."""

    message: str = Field(description='message to print')
    repeat: int = Field(1, description='number of times to repeat message')
```
The [`Config`][webs.config.Config] class supports required arguments without default values (e.g., `message`) and optional arguments with default values (e.g., `repeat`).

The final file, `webs/wf/foobar/workflow.py`, will contain the core workflow logic.
Task functions and workflow code can be included here or in another module within `webs/wf/foobar`.
For example, this trivial example workflow will be entirely contained within `webs/wf/foobar/workflow.py` but more complex workflows may want to split up the code across many modules.
Nonetheless, the entry point for the workflow will be in the [`run()`][webs.workflow.Workflow.run] method inside of `webs/wf/foorbar/workflow.py`.
We'll first list the code, then discuss the important sections.
```python title="webs/wf/foobar/workflow.py" linenums="1"
from __future__ import annotations

import logging
import pathlib
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.foobar.config import FoobarWorkflowConfig
from webs.workflow import register

logger = logging.getLogger(__name__)


def print_message(message: str) -> None:
    """Print a message."""
    logger.log(WORK_LOG_LEVEL, message)


@register()
class FoobarWorkflow(ContextManagerAddIn):
    """Foobar workflow.

    Args:
        message: Message to print.
        repeat: Number of times to repeat the message.
    """

    name = 'foobar'
    config_type = FoobarWorkflowConfig

    def __init__(self, message: str, repeat: int = 1) -> None:
        self.message = message
        self.repeat = repeat
        super().__init__()

    @classmethod
    def from_config(cls, config: FoobarWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(message=config.message, repeat=config.repeat)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        for _ in range(self.repeat):
            task = executor.submit(print_message, self.message)
            task.result()  # Wait on task to finish
```

1. Workflows in WEBS are composed on tasks which are just Python functions.
   Here, our task is the `print_message` function.
2. The `FoobarWorkflow` implements the [`Workflow`][webs.workflow.Workflow] protocol.
   Importantly, `FoobarWorkflow` is decorated by the `@register()` decorator which tells WEBS to add this workflow to the command line interface (CLI).
   The `@register()` decorator uses the `name = 'foobar'` attribute of `FoobarWorkflow` as the name used in the CLI to select this workflow.
   The `config_type = FoobarWorkflowConfig` attribute tells the CLI that the `FoobarWorkflowConfig` we defined in `webs/wf/foobar/config.py` is the configuration to create command line arguments from.
3. When the CLI is invoked for this workflow, the arguments will be parsed into a `FoobarWorkflowConfig` and then `FoobarWorkflow.from_config()` will be used to instantiate the workflow runner.
4. Once `FoobarWorkflow` is instantiated from the config, `FoobarWorkflow.run()` will be invoked.
   This method takes two arguments: a [`WorkflowExecutor`][webs.executor.workflow.WorkflowExecutor] and a path to the invocations run directory.
   Workflows are free to use the run directory as needed, such as to store result files.

## Workflow Executor

The [`WorkflowExecutor`][webs.executor.workflow.WorkflowExecutor] is the key abstraction of the WEBS framework.
The CLI arguments provided by the user for the compute engine, data management, and task logging logic are used to create a [`WorkflowExecutor`][webs.executor.workflow.WorkflowExecutor] instance which is then provided to the workflow.
[`WorkflowExecutor.submit()`][webs.executor.workflow.WorkflowExecutor.submit] is the primary method that workflows will use to execute tasks asynchronously.
This method returns a [`TaskFuture`][webs.executor.workflow.TaskFuture] object with a [`result()`][webs.executor.workflow.TaskFuture.result] which will wait on the task to finish and return the result.
Alternatively, [`WorkflowExecutor.map()`][webs.executor.workflow.WorkflowExecutor.map] can be used to map a task onto a sequence of inputs, compute the tasks in parallel, and gather the results.
Importantly, a [`TaskFuture`][webs.executor.workflow.TaskFuture] can also be passed as input to another tasks.
Doing so indicates to the [`WorkflowExecutor`][webs.executor.workflow.WorkflowExecutor] that there is a dependency between those two tasks.

## Running a Workflow

Once a workflow is created and registered within WEBS, the workflow is available within the CLI.
```bash
python -m webs.run foobar --help
```
Using `foobar` as the first positional argument indicates we want to execute the `foobar` workflow, and `--help` will print all of the required and optional arguments of the workflow.
The arguments will be separated into sections, such as for arguments specific to the `foobar` workflow or for executor arguments.

The following command will execute the workflow to print "Hello, World!" three times.
We specify the `thread-pool` executor because this will allow our printing to show up in the main process.
```bash
$ python -m webs.run foobar --message 'Hello, World!' --repeat 3 --executor thread-pool
RUN   (webs.run) :: Starting workflow (name=foobar)
...
WORK  (webs.wf.foobar.workflow) :: Hello, World!
WORK  (webs.wf.foobar.workflow) :: Hello, World!
WORK  (webs.wf.foobar.workflow) :: Hello, World!
RUN   (webs.run) :: Finished workflow (name=foobar, runtime=0.00s)
```
