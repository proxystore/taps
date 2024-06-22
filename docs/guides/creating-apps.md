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
[`AppConfig`][taps.apps.AppConfig] and [`Config`][taps.apps.App].
The [`AppConfig`][taps.apps.AppConfig] is a Pydantic [`BaseModel`][pydantic.BaseModel] containing all configuration options that should be exposed via the CLI.
The [`App`][taps.apps.App] has a [`run()`][taps.apps.App.run] method which is the entry point to running the applications.

### The `App`

All applications are submodules of `taps/apps/`.
Our `foobar` application is simple so we will create a single file module named `taps/apps/foobar.py`.
More complex applications can create a subdirectory containing many submodules.


```python title="taps/apps/foobar.py" linenums="1"
from __future__ import annotations

import logging
import pathlib

from taps.engine import Engine
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)


def print_message(message: str) -> None:
    """Print a message."""
    logger.log(APP_LOG_LEVEL, message)


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

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
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
2. The `FoobarApp` implements the [`App`][taps.apps.App] protocol.
3. The `close()` method can be used to close any stateful connection objects created in `__init__` or perform any clean up if needed.
4. Once `FoobarApp` is instantiated by the CLI, `FoobarApp.run()` will be invoked.
   This method takes two arguments: an [`Engine`][taps.engine.Engine] and a path to the invocations run directory.
   Applications are free to use the run directory as needed, such as to store result files.

The [`Engine`][taps.engine.Engine] is the key abstraction of the TaPS framework.
The CLI arguments provided by the user for the compute engine, data management, and task logging logic are used to create an [`Engine`][taps.engine.Engine] instance which is then provided to the application.
[`Engine.submit()`][taps.engine.Engine.submit] is the primary method that application will use to execute tasks asynchronously.
This method returns a [`TaskFuture`][taps.engine.TaskFuture] object with a [`result()`][taps.engine.TaskFuture.result] which will wait on the task to finish and return the result.
Alternatively, [`Engine.map()`][taps.engine.Engine.map] can be used to map a task onto a sequence of inputs, compute the tasks in parallel, and gather the results.
Importantly, a [`TaskFuture`][taps.engine.TaskFuture] can also be passed as input to another tasks.
Doing so indicates to the [`Engine`][taps.engine.Engine] that there is a dependency between those two tasks.

### The `AppConfig`

An [`AppConfig`][taps.apps.AppConfig] is registered with the TaPS CLI and defines (1) what arguments should be available in the CLI and (2) how to construct and [`App`][taps.apps.AppConfig] from the configuration.
Each [`App`][taps.apps.App] definition has a corresponding [`AppConfig`][taps.apps.AppConfig] defined in `taps/apps/configs/`.
Here, we'll create a file `taps/apps/configs/foobar.py` for our `FoobarConfig`.
This configuration will contain all of the parameters that the user is required to provide and any optional parameters.

```python title="taps/apps/configs/foobar.py" linenums="1"
from __future__ import annotations

from typing import Literal

from pydantic import Field

from taps.apps import App
from taps.apps import AppConfig
from taps.plugins import register


@register('app')
class FoobarConfig(AppConfig):
    """Foobar application configuration."""

    name: Literal['foobar'] = 'foobar'
    message: str = Field(description='message to print')
    repeat: int = Field(1, description='number of times to repeat message')

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.foobar import FoobarApp

        return FoobarApp(message=self.message, repeat=self.repeat)
```

1. The [`@register()`][taps.plugins.register] decorator registers the `FoobarConfig` with the TaPS as an `'app'` plugin.
   The `name` attribute of the config is the name under which the application will be available in the CLI.
   For example, here we can use `python -m taps.run --app foobar {args}` to run our application.
2. The [`AppConfig`][taps.apps.AppConfig] class supports required arguments without default values (e.g., `message`) and optional arguments with default values (e.g., `repeat`).
3. The [`get_app()`][taps.apps.AppConfig.get_app] method is required and is invoked by the CLI to create an [`App`][taps.apps.App] instance.
4. **Note:** `FoobarApp` is imported inside of [`get_app()`][taps.apps.AppConfig.get_app] to delay importing dependencies specific to the application until the user has decided which application they want to execute.

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
python -m taps.run --app foobar --help
```
The `--help` flag will print all of the required and optional arguments as specified in the `FoobarConfig` because `--app foobar` was specified.
If no app is specified, `--help` will just print the available apps that can be used.
The arguments will be separated into sections, such as for arguments specific the `foobar` app or for executor arguments.

The following command will execute the application to print "Hello, World!" three times.
We specify the `thread-pool` executor because this will allow our printing to show up in the main process.
```bash
$ python -m taps.run --app foobar --app.message 'Hello, World!' --app.repeat 3 --engine.executor thread-pool
RUN   (taps.run) :: Starting application (name=foobar)
...
APP  (taps.apps.foobar) :: Hello, World!
APP  (taps.apps.foobar) :: Hello, World!
APP  (taps.apps.foobar) :: Hello, World!
RUN   (taps.run) :: Finished application (name=foobar, runtime=0.00s)
```
