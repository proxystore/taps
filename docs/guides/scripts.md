# Custom Benchmark Scripts

The `python -m taps.run` CLI has limited support for repeating benchmarks while varying parameters (e.g., executor types, application parameters, etc.).
More sophisticated benchmarks can be performed through custom benchmark scripts that utilize the TaPS API.

This guide provides some examples scripts and pointers to get started.
Each example will use the [Cholesky Factorization](../apps/cholesky.md) application and Python's [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor] for task execution.

## Running an `App`

A TaPS [`App`][taps.apps.App] can be executed directly using [`App.run()`][taps.apps.App.run] which requires an [`Engine`][taps.engine.Engine] and a run directory (required, but not actually used by all apps).
This is the most direct and manual means of running an application.

!!! note

    This example does not configure the [`Engine`][taps.engine.Engine] with a [`RecordLogger`][taps.record.RecordLogger].
    A [`RecordLogger`][taps.record.RecordLogger], such as [`JSONRecordLogger`][taps.record.JSONRecordLogger], is necessary to take advantage of the task execution information collection of the [`Engine`][taps.engine.Engine].

```python title="app_example.py" linenums="1"
import contextlib
import pathlib
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

from taps.apps.cholesky import CholeskyApp
from taps.engine import Engine
from taps.executor.utils import FutureDependencyExecutor
from taps.logging import init_logging


def main() -> int:
    init_logging()

    app = CholeskyApp(matrix_size=100, block_size=25)
    executor = FutureDependencyExecutor(ProcessPoolExecutor(max_workers=4))
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    run_dir = pathlib.Path.cwd() / 'runs' / timestamp

    with contextlib.closing(app), Engine(executor) as engine:
        app.run(engine, run_dir)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
```
This script can be executed directly.
```bash
python app_example.py
```

## TaPS Run Helper

Internally, the `python -m taps.run` CLI calls the [`run()`][taps.run.main.run] function which handles creating all of the benchmarking objects based on a [`Config`][taps.run.config.Config] and executing the benchmark.
Thus, [`run()`][taps.run.main.run] is easier to use but is also higher level and therefore less customizable.

```python title="run_example.py" linenums="1"
import pathlib
from datetime import datetime

from taps.apps.configs.cholesky import CholeskyConfig
from taps.engine import EngineConfig
from taps.executor.python import ProcessPoolConfig
from taps.logging import init_logging
from taps.run.config import Config
from taps.run.main import run
from taps.run.utils import change_cwd


def main() -> int:
    init_logging()

    config = Config(
        app=CholeskyConfig(matrix_size=100, block_size=25),
        engine=EngineConfig(executor=ProcessPoolConfig(max_processes=4)),
    )
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    with change_cwd(pathlib.Path.cwd() / 'runs' / timestamp) as run_dir:
        run(config, run_dir)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
```
This script can be executed directly.
```bash
python run_example.py
```

A matrix of benchmark configurations can be performed, for example, by altering the [`Config`][taps.run.config.Config] each time and creating a new run directory (to avoid overwriting previous configurations).

!!! warning

    A new [`Engine`][taps.engine.Engine], and therefore executor, will be created each time [`run()`][taps.run.main.run] is invoked.
    This can be inefficient for certain executors which take extended time to startup or shutdown.

## Parameter Matrix Example

This example shows how to repeat benchmarks with the [Cholesky Factorization](../apps/cholesky.md) application for a matrix of application parameters (`matrix_size` and `block_size`) using [`itertools.product`][itertools.product].
Here, we reuse the same [`Engine`][taps.engine.Engine] across all runs since the engine parameters are not being changed.

```python title="matrix_example.py" linenums="1"
import contextlib
import itertools
import logging
import pathlib
from concurrent.futures import ProcessPoolExecutor

from taps.apps.cholesky import CholeskyApp
from taps.engine import Engine
from taps.executor.utils import FutureDependencyExecutor
from taps.logging import init_logging
from taps.logging import RUN_LOG_LEVEL

logger = logging.getLogger(__name__)


def main() -> int:
    init_logging()

    matrix_sizes = [100, 200, 300]
    blocks = [1, 2, 4]
    executor = FutureDependencyExecutor(ProcessPoolExecutor(max_workers=4))

    with Engine(executor) as engine:
        for matrix_size, n_blocks in itertools.product(matrix_sizes, blocks):
            logger.log(
                RUN_LOG_LEVEL,
                'Starting new run '
                f'(matrix_size={matrix_size}, blocks={n_blocks})',
            )

            app = CholeskyApp(
                matrix_size=matrix_size,
                block_size=matrix_size // n_blocks,
            )
            run_dir = f'matrix-{matrix_size}x{matrix_size}-blocks-{n_blocks}'
            run_dir = pathlib.Path.cwd() / 'runs' / run_dir

            with contextlib.closing(app):
                app.run(engine, run_dir)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
```
This script can be executed directly.
```bash
python matrix_example.py
```
