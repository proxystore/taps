# Quick Start

TaPS is a standardized framework for evaluating task-based execution frameworks and data management systems using a suite a real and synthetic scientific applications.

## Installation

```bash
git clone https://github.com/proxystore/taps
cd taps
python -m venv venv
. venv/bin/activate
pip install -e .
```

Documentation on installing for local development is provided in [Contributing](contributing/index.md).

## Usage

Applications can be executed from the CLI.
```bash
python -m taps.run --app {name} {args}
```
See `python -m taps.run --help` for a list of applications.

### Example App

The [Cholesky Factorization](apps/cholesky.md) app, for example, can be run using `--app cholesky` and the required arguments.
```bash
python -m taps.run --app cholesky --app.matrix_size 100 --app.block_size 25
```

!!! note

    This `cholesky` app requires having installed TaPS with the `[cholesky]` option.
    ```
    pip install -e .[cholesky]
    ```

Many execution options can be altered directly from the command line.
The above example, by default, used a [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor] but we can switch to a different executor easily with the `--engine.executor` flag.
```bash
python -m taps.run \
    --app cholesky --app.matrix_size 100 --app.block_size 25 \
    --engine.executor thread-pool
```

### Config Files

Alternatively, applications can be configured using a TOML configuration file.

```toml title="config.toml" linenums="1"
[app]
name = "cholesky"
matrix_size = 100
block_size = 25

[engine]
task_record_file_name = "tasks.jsonl"

[engine.executor]
name = "process-pool"
max_processes = 10

[engine.filter]
name = "null"

[engine.transformer]
name = "null"

[run]
dir_format = "runs/{name}_{executor}_{timestamp}"

[logging]
file_level = "WARNING"
file_name = "log.txt"
level = "INFO"
```

To execute from a config, use the `-c/--config` option.
```bash
python -m taps.run --config config.toml
```

Options provided via the CLI will override those options present in a config file.

## Apps

Checkout the [Application Guides](apps/index.md) to learn about all of the different benchmarking applications provided by TaPS.
