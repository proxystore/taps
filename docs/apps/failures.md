# Failure Injection

The `failures` application randomly injects task failure scenarios into another TaPS application.
This is useful to understanding how an application or execution engine handles certain forms of task failures.

!!! warning

    Some of the error scenarios may have unexpected consequences to other actively running programs.
    For example:

      - `MANAGER_KILLED` will kill the parent process that a task is executing within.
      - `MEMORY` will continually consume memory until an error is raised. This can cause other applications to crash.
      - `NODE_KILLED` attempt to kill other processes on the node to simulate failures.
      - `RANDOM` will select a random, potentially dangerous, failure mode.
      - `WORKER_KILLED` will kill the process that a task is executing within.

    Please be careful when using this application, and run the application in an isolated environment (e.g., a container or ephemeral node).

## Installation

The `failures` application requires executing a base application (the application into which failures are injected), and some base applications have additional requirements.
For example, to use `failures` with the `cholesky` application, install TaPS using:
```bash
pip install -e .[cholesky]
```

!!! warning

    The `failures` app is not compatible with `dill==0.3.6` which, as of writing, is the pinned version installed by `globus-compute-sdk`.
    To use the `failures` app, manually upgrade `dill`:
    ```bash
    pip install --upgrade dill==0.3.8
    ```
    It is still possible to use Globus Compute with newer `dill` versions but you must ensure the same version of `dill` is installed on all endpoints.
    See [Issue #155](https://github.com/proxystore/taps/issues/155){target=_blank} for more information.

## Data

Data requirements depend on the base application that failures are injected into.

## Example

The base application name is specified using `--app.base` and the corresponding configuration must be provided as a JSON string to `--app.config`.

```bash
python -m taps.run --app failures \
    --app.base cholesky \
    --app.config '{"matrix_size": 100, "block_size": 50}' \
    --app.failure-rate 0.5 --app.failure-type dependency \
    --engine.executor process-pool --engine.executor.max-processes 4
```

Alternatively, the app can be configured using a TOML file.

```toml title="config.toml"
[app]
name = "failures"
base = "cholesky"
failure_rate = 0.5
failure_type = "dependency"

[app.config]
matrix_size = 100
block_size = 50

[engine.executor]
name = "process-pool"
max_processes = 4
```
```bash
python -m taps.run --config config.toml
```
