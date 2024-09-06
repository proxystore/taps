# Frequently Asked Questions

*[Open a new issue](https://github.com/proxystore/taps/issues){target=_bank} if you have a question not answered in the FAQ, Guides, or API docs.*

## Applications

### What applications are available in TaPS?

The complete list of available applications is available in the help text of the CLI.
```bash
$ python -m taps.run --help
```
Check out the [Application Guides](apps/index.md) to learn more about the specific applications.
If you want to add a new application, check out the [Benchmarking Apps](guides/apps.md).

### Why do I get invalid path errors?

TaPS creates a unique run directory each time an application is executed.
The location of the directory is determined by [`Config.run.dir_format`][taps.run.config.RunConfig] and [`make_run_dir()`][taps.run.config.make_run_dir].
Before starting the application, TaPS will change the current working directory of the process to the run directory.
This can cause relative paths configured in the application to break.

The [`AppConfig`][taps.apps.AppConfig] class will resolve all [`pathlib.Path`][pathlib.Path] types to absolute paths before changing working directories to avoid incorrect filepaths, and applications should be careful to create all filepaths relative to the `run_dir` value provided to [`App.run()`][taps.apps.App.run].

If you encounter a similar issue in an existing TaPS application, please [open a GitHub issue](https://github.com/proxystore/taps/issues){target=_bank}.

## Debugging

### How to enable debug logging?

The TaPS CLI defaults to logging `INFO` messages and above to `stdout` and log files.
These can be controlled independently with `--logging.level <LEVEL>` and `--logging.file-level <LEVEL>` (the log file level defaults to the `stdout` level if unspecified).
Setting the logging level to `DEBUG` or `TRACE` can be helpful for debugging.
Note that this will also set the logging level for third-party libraries which can result in *a lot* of log messages.
Within TaPS, `TRACE` will enable per-task logging so it may be helpful to set `--logging.level INFO --logging.file-level TRACE` to avoid clobbering the terminal output.
