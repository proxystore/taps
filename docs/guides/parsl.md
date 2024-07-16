# Parsl Configuration

TaPS provides (as of writing) two different executor configuration types for using [Parsl](https://github.com/parsl/parsl){target=_blank}: `parsl-local` and `parsl-htex`.

## `parsl-local` Config

The `parsl-local` executor is the easiest way to run a benchmark using Parsl as the executor.
The `parsl-local` executor corresponds to the [`ParslLocalConfig`][taps.executor.parsl.ParslLocalConfig] which creates a [`HighThroughputExecutor`][parsl.executors.HighThroughputExecutor] configured on the local node.

For example, configuring Parsl on the local node with eight workers is easy.
```bash
python -m taps.run \
    --app cholesky --app.matrix_size 100 --app.block_size 25 \
    --engine.executor parsl-local --engine.executor.workers 8
```

## `parsl-htex` Config

More advanced Parsl configurations will need to use the `parsl-htex` executor which corresponds to the [`ParslHTExConfig`][taps.executor.parsl.ParslHTExConfig].
The `parsl-htex` executor still creates a [`HighThroughputExecutor`][parsl.executors.HighThroughputExecutor], but the full configuration options are exposed (e.g., addresses, providers, launchers, etc.).

!!! tip

    Due to the complexity of configuring the `parsl-htex`, a [TOML configuration file](config.md#toml-configuration) should be used.
    Not all Parsl configuration options will be available in the CLI parser arguments.

The following is a simple configuration that mostly defaults to the defaults set in Parsl's [`HighThroughputExecutor`][parsl.executors.HighThroughputExecutor], except for `max_workers_per_node` and `address` which are specified.

```toml title="parsl-config.toml" linenums="1"
[engine.executor]
name = "parsl-htex"

[engine.executor.htex]
max_workers_per_node = 8

[engine.executor.htex.address]
kind = "address_by_hostname"
```

```bash
python -m taps.run \
    --app cholesky --app.matrix_size 100 --app.block_size 25 \
    --config parsl-config.toml
```

Extra options not explicitly defined in the various sub-configs of [`ParslHTExConfig`][taps.executor.parsl.ParslHTExConfig] can still be provided and will be passed as keyword arguments to the corresponding Parsl classes.

These configuration semantics are similar to the [`GlobusComputeEngine`](https://globus-compute.readthedocs.io/en/latest/endpoints/endpoint_examples.html#globuscomputeengine){target=_blank} which wraps Parl's [`HighThroughputExecutor`][parsl.executors.HighThroughputExecutor].

## Examples

### Polaris at ALCF

The following configuration is an example for the [Polaris GPU cluster at ALCF](https://www.alcf.anl.gov/polaris){target=_blank}.
The configuration is based on the [Globus Compute endpoint](https://globus-compute.readthedocs.io/en/latest/endpoints/endpoint_examples.html#polaris-alcf) example.

```toml title="parsl-config.toml" linenums="1"
[engine.executor]
name = "parsl-htex"

[engine.executor.htex]
max_workers_per_node = 4

[engine.executor.htex.address]
kind = "address_by_interface"
ifname = "bond0"

[engine.executor.htex.provider]
kind = "PBSProProvider"
account = {{ ALCF_ALLOCATION }}
cpus_per_node = 32
init_blocks = 0
max_blocks = 2
min_blocks = 0
nodes_per_block = 1
queue = "debug-scaling"
scheduler_options = "#PBS -l filesystems=home:grand:eagle"
select_options = "ngpus=4"
walltime = "01:00:00"
worker_init = {{ COMMAND_STRING }}

[engine.executor.htex.provider.launcher]
kind = "MpiExecLauncher"
bind_cmd = "--cpu-bind"
overrides = "--depth=64 --ppn=1"
```
