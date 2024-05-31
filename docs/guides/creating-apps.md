# Create an Application

This guide describes creating a benchmarking application within the TaPS framework.

## Installation

A development environment needs to be configured first.
Fork the repository and clone your fork locally.
Then, configure a virtual environment with the TaPS package and development dependencies.

```bash
python -m venv venv
. venv/bin/activate
pip install -e .[dev,docs]
```

See [Getting Started for Local Development](../contributing/index.md#getting-started-for-local-development) for detailed instructions on running the linters and continuous integration tests.

## Application Structure

Our example application is going to be called `foobar`.
All applications in TaPS are composed of two required components:
[`AppConfig`][taps.app.AppConfig] and [`Config`][taps.app.App].
The [`AppConfig`][taps.app.AppConfig] is a Pydantic [`BaseModel`][pydantic.BaseModel] with some extra functionality for constructing a config instance from command line arguments.
The [`App`][taps.app.App] has a [`run()`][taps.app.App.run] method which is the entry point to running the applications.

### The `App`

All applications are submodules of `taps/apps/`.
Our `foobar` application is simple so we will create a single file module named `taps/apps/foobar.py`.
More complex applications can create a subdirectory containing many submodules.


```python title="taps/apps/foobar.py" linenums="1"
from __future__ import annotations

import logging
import pathlib

from taps.engine import AppEngine
from taps.logging import WORK_LOG_LEVEL

logger = logging.getLogger(__name__)


def print_message(message: str) -> None:
    """Print a message."""
    logger.log(WORK_LOG_LEVEL, message)


class FoobarApp:
    """Foobar application.

    Args:
        message: Message to print.
        repeat: Number of times to repeat the message.
    """

    def __init__(self, message: str, repeat: int = 1) -> None:
        self.message = message
        self.repeat = repeat

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: AppEngine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        for _ in range(self.repeat):
            task = engine.submit(print_message, self.message)
            task.result()  # Wait on task to finish
```

1. Applications in TaPS are composed on tasks which are just Python functions.
   Here, our task is the `print_message` function.
2. The `FoobarApp` implements the [`App`][taps.app.App] protocol.
3. The `close()` method can be used to close any stateful connection objects create in `__init__` or perform any clean up if needed.
4. Once `FoobarApp` is instantiated by the CLI, `FoobarApp.run()` will be invoked.
   This method takes two arguments: a [`AppEngine`][taps.engine.AppEngine] and a path to the invocations run directory.
   Workflows are free to use the run directory as needed, such as to store result files.

The [`AppEngine`][taps.engine.AppEngine] is the key abstraction of the TaPS framework.
The CLI arguments provided by the user for the compute engine, data management, and task logging logic are used to create a [`AppEngine`][taps.engine.AppEngine] instance which is then provided to the application.
[`AppEngine.submit()`][taps.engine.AppEngine.submit] is the primary method that application will use to execute tasks asynchronously.
This method returns a [`TaskFuture`][taps.engine.TaskFuture] object with a [`result()`][taps.engine.TaskFuture.result] which will wait on the task to finish and return the result.
Alternatively, [`AppEngine.map()`][taps.engine.AppEngine.map] can be used to map a task onto a sequence of inputs, compute the tasks in parallel, and gather the results.
Importantly, a [`TaskFuture`][taps.engine.TaskFuture] can also be passed as input to another tasks.
Doing so indicates to the [`AppEngine`][taps.engine.AppEngine] that there is a dependency between those two tasks.

### The `AppConfig`

An `AppConfig` is registered with the TaPS CLI and defines (1) what arguments should be available in the CLI and (2) how to construct and `App` from the configuration.
Each `App` definition has a corresponding `AppConfig` defined in `taps/run/apps/`.
Here, we'll create a file `taps/run/apps/foobar.py` for our `FoobarConfig`.
This configuration will contain all of the parameters that the user is required to provide and any optional parameters..

```python title="taps/run/apps/foobar.py" linenums="1"
from __future__ import annotations

from pydantic import Field

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='foobar')
class FoobarConfig(AppConfig):
    """Foobar application configuration."""

    message: str = Field(description='message to print')
    repeat: int = Field(1, description='number of times to repeat message')

    def create_app(self) -> App:
        from taps.apps.foobar import FoobarApp

        return FoobarApp(message=self.message, repeat=self.repeat)
```

1. The [`@register_app()`][taps.run.apps.registry.register_app] decorator registers the `FoobarConfig` with the TaPS CLI.
   The name specified in the decorator is the name under which the application will be available in the CLI.
   For example, here we can use `python -m taps.run foobar {args}` to run our application.
2. The [`AppConfig`][taps.app.AppConfig] class supports required arguments without default values (e.g., `message`) and optional arguments with default values (e.g., `repeat`).
3. The [`create_app()`][taps.app.AppConfig.create_app] method is required and is invoked by the CLI to create an [`App`][taps.app.App] instance.
4. **Note:** `FoobarApp` is imported inside of [`create_app()`][taps.app.AppConfig.create_app] to delay importing dependencies specific to the application until the user has decided which application they want to execute.

### Dependencies

Applications which require extra dependencies should do one of the following.

1. Add an optional dependencies section to `pyproject.toml`.
   If the dependencies are pip installable, add a new section with the name of the application to the `[project.optional-dependencies]` section in `pyproject.toml`.
   For example:
   ```toml
   [project.optional-depedencies]
   foobar = ["my-dependency"]
   ```
2. Add installation instructions to the application's documentation.
   More complex applications may have dependencies which are not installable with pip.
   In this case, instructions should be provided in documentation, discussed in the next section.

### Documentation

Each application should have an associated documentation page which describes (1) what the application is based on, (2) what the application does, and (3) how to run the application.
Application documentation is written using markdown files in `docs/apps/`.
For example, our `foobar` application will have a file called `docs/apps/foobar.md`.
Once your markdown file is written, it needs to be added to the documentation navigation tree.

First, modify `docs/apps/index.md` to contain a link to the file.
```markdown
- [Foobar](foobar.md)
```
Then, modify the navigation tree in `mkdocs.yml` to contain the path to the markdown file within the "Apps" section of the docs.
```yaml
nav:
  - App:
      - apps/index.md
      - Foobar: apps/foobar.md
```
Please keep the lists in each of these files alphabetized.

Once these files have been added, you can build the documentation.
This requires having installed TaPS with the `docs` option (e.g., `pip install .[docs]`).
```bash
mkdocs build --strict
```
You will be able to inspect that your page is visible in the "Apps" section of the docs and is formatted correctly.

## Running the Application

Once an application is created and registered within TaPS, the application is available within the CLI.
```bash
python -m taps.run foobar --help
```
Using `foobar` as the first positional argument indicates we want to execute the `foobar` application, and `--help` will print all of the required and optional arguments as specified in the `FoobarConfig`.
The arguments will be separated into sections, such as for arguments specific to `foobar` or for executor arguments.

The following command will execute the application to print "Hello, World!" three times.
We specify the `thread-pool` executor because this will allow our printing to show up in the main process.
```bash
$ python -m taps.run foobar --message 'Hello, World!' --repeat 3 --executor thread-pool
RUN   (taps.run) :: Starting application (name=foobar)
...
WORK  (taps.apps.foobar) :: Hello, World!
WORK  (taps.apps.foobar) :: Hello, World!
WORK  (taps.apps.foobar) :: Hello, World!
RUN   (taps.run) :: Finished application (name=foobar, runtime=0.00s)
```
