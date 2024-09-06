# Benchmark Configuration

A benchmark run in TaPS is defined by a [`Config`][taps.run.config.Config], a hierarchical pydantic [`BaseModel`][pydantic.BaseModel] containing the full configurations for the benchmarking application, engine, and environment.

## Configuring a Benchmark

Configuration models contains a mix of required and optional fields (i.e., fields with a reasonable default value).
Required and optional fields can be configured in two ways: as CLI parameters or via TOML files.
CLI parameters and TOML files can be intermixed, with CLI parameters taking precedences over fields in TOML files.

Next, we'll describe three ways to configure a benchmark using the [Cholesky Factorization](../apps/cholesky.md) and a [Dask Executor][taps.executor.dask].

### CLI Parameters

The simplest way to run a benchmark is with CLI parameters.
```bash
python -m taps.run --app cholesky --app.matrix-size 100 --app.block-size 25 \
    --engine.executor dask --engine.executor.workers 4
```
Here, `app`, `app.matrix-size`, and `app.block-size` are required fields because they have no default values.
In contrast, `engine.executor` and `engine.executor.worker` are not required because they have default; however, we are overriding the default values (from `process-pool` to `dask` and from `None` to `4`, respectively).

Executing the above command will print the resulting [`Config`][taps.run.config.Config] and write it to a `config.toml` file in the run directory.
For example, you will see the following log line.
```
[2024-07-10 11:24:18.081] RUN   (taps.run) :: Configuration:
app:
  name: 'cholesky'
  block_size: 25
  matrix_size: 100
engine:
  executor:
    name: 'dask'
    daemon_workers: True
    scheduler: None
    use_threads: False
    workers: 4
  filter: None
  task_record_file_name: 'tasks.jsonl'
  transformer: None
logging:
  file_level: 'INFO'
  file_name: 'log.txt'
  level: 'INFO'
run:
  dir_format: 'runs/{name}_{executor}_{timestamp}'
```
All of the fields we specified are here, as well as all of the default fields.
The `config.toml` file will contain the same information but formatted as TOML.

!!! note

    The CLI supports dashes (`-`) and underscores (`_`) for field names, but underscores are required in TOML config files.

### TOML Configuration

Alternatively, TOML configuration files can be used to execute benchmarks using the `--config <PATH>` option.
The following configuration file is equivalent to the CLI parameters in the prior example.
```toml title="config.toml"
[app]
name = "cholesky"
matrix_size = 100
block_size = 25

[engine.executor]
name = "dask"
workers = 4
```
To run:
```bash
python -m taps.run --config config.toml
```

### Mixing CLI and TOML

A benchmark can be configured through a mix of CLI parameters and config files.
CLI parameters always take precedence over fields in config files.

For example, we can provide the above `config.toml` and then override the `app.block_size` field to via the CLI.
```bash
python -m taps.run --config config.toml --app.block_size 50
```

## Example: Comparing Executors

A common use case for TaPS is evaluating the same application with a set of different optimizers.
Using multiple configuration files provides an easy mechanism to do so.

First, we create a configuration file for our application which will be used in every run.
```toml title="app-config.toml"
[app]
name = "cholesky"
matrix_size = 100
block_size = 25
```

Then, we can create multiple configuration files for each executor we want to evaluate.
As an example, we'll create one for Python's process pool, Parsl, and Dask.
```toml title="process-config.toml"
[engine.executor]
name = "process-pool"
max_processes = 4
```
```toml title="parsl-config.toml"
[engine.executor]
name = "parsl"
workers = 4
```
```toml title="dask-config.toml"
[engine.executor]
name = "dask"
workers = 4
```

The benchmarks can be executed, alternating the executor configuration each time.
```bash
python -m taps.run --config app-config.toml process-config.toml
python -m taps.run --config app-config.toml parsl-config.toml
python -m taps.run --config app-config.toml dask-config.toml
```

!!! note

    When multiple configuration files are provided, files will be parsed in order with latter files overriding previous files when the same field is present in both.

## Environment Variables

Some applications, executors, etc. may require configuring environment variables.
The TaPS configuration system provides a mechanism to configure environment variables to be set during execution of a benchmark.
The benefit of setting environment variables within the TaPS configuration system (rather than exporting variables manually) is that those variables will be included in the resulting configuration files that can be shared.

For example, we can set `ENV_VAR_1="foo"` and `ENV_VAR_2="bar"` variables using a JSON CLI parameter or fields in a config file.
```bash title="CLI Environment Variables"
python -m taps.run {args} --run.env-vars '{"ENV_VAR_1": "foo", "ENV_VAR_2": "bar"}'
```
```toml title="TOML Environment Variables"
[run.env_vars]
ENV_VAR_1 = "foo"
ENV_VAR_2 = "bar"
```
`ENV_VAR_1="foo"` and `ENV_VAR_2="bar"` will be set when the application starts and then unset when finished.
If an environment variable is already populated, the original value will be cached and restored when the application finishes.
