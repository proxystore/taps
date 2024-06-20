from __future__ import annotations

import argparse
import contextlib
import functools
import logging
import os
import pathlib
import sys
import time
from collections.abc import MutableMapping
from typing import Any
from typing import Callable
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    import tomllib
else:  # pragma: <3.11 cover
    import tomli as tomllib

from pydantic import create_model
from pydantic import Field
from pydantic_settings import CliSettingsSource

from taps import plugins
from taps.engine import AppEngineConfig
from taps.logging import init_logging
from taps.logging import RUN_LOG_LEVEL
from taps.run.config import Config
from taps.run.config import make_run_dir

logger = logging.getLogger('taps.run')


def _flatten_dict(
    d: MutableMapping[str, Any],
    parent_key: str = '',
    separator: str = '.',
) -> dict[str, Any]:
    items: list[tuple[str, Any]] = []
    for key, value in d.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(
                _flatten_dict(value, new_key, separator=separator).items(),
            )
        else:
            items.append((new_key, value))
    return dict(items)


def _parse_toml_options(filepath: str | None) -> dict[str, Any]:
    if filepath is None:
        return {}

    with open(filepath, 'rb') as f:
        options = tomllib.load(f)

    return _flatten_dict(options)


def _add_argument(
    parser: argparse.ArgumentParser,
    *names: str,
    **kwargs: Any,
) -> None:
    if any(name.endswith('.name') for name in names):
        return

    parser.add_argument(*names, **kwargs)


def _add_argument_group(
    parser: argparse.ArgumentParser,
    **kwargs: Any,
) -> argparse._ArgumentGroup:
    title = kwargs.get('title', None)

    for group in parser._action_groups:
        if group.title == title:
            return group

    return parser.add_argument_group(**kwargs)


def _make_settings_cls(options: dict[str, Any]) -> type[Config]:
    app_name = options.get('app.name')
    assert isinstance(app_name, str)
    app_cls = plugins.get_app_configs()[app_name]

    executor_name = options.get('engine.executor.name', 'process-pool')
    executor_cls = plugins.get_executor_configs()[executor_name]

    filter_name = options.get('engine.filter.name', 'null')
    filter_cls = plugins.get_filter_configs()[filter_name]

    transformer_name = options.get('engine.transformer.name', 'null')
    transformer_cls = plugins.get_transformer_configs()[transformer_name]

    engine_cls = create_model(
        'AppEngineConfig',
        executor=(
            executor_cls,
            Field(description=f'{executor_name} executor options'),
        ),
        filter=(
            filter_cls,
            Field(description=f'{filter_name} filter options'),
        ),
        transformer=(
            transformer_cls,
            Field(description=f'{transformer_name} transformer options'),
        ),
        __base__=AppEngineConfig,
        __doc__=AppEngineConfig.__doc__,
    )

    return create_model(
        'Config',
        app=(app_cls, Field(description=f'{app_name} app options')),
        engine=(engine_cls, Field(description='engine options')),
        __base__=Config,
        __doc__=Config.__doc__,
    )


class _ArgparseFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


def parse_args_to_config(argv: Sequence[str]) -> Config:
    """Parse sequence of string arguments into a config.

    Args:
        argv: Sequence of string arguments.

    Returns:
        Configuration.
    """
    apps = plugins.get_app_configs()

    parser = argparse.ArgumentParser(
        description="""\
Task Performance Suite (TaPS) CLI.

Application benchmarks can be configured via CLI options, a TOML
configuration file, or a mix of both. CLI options take precedence
over configuration files.

The default behavior of -h/--help is to show only the minimally
relevant set of options. For example, only the process-pool
executor options will be shown if --engine.executor process-pool
is specified; the options for other executors will be suppressed.
This behavior applies to all plugin types.
""",
        prog='python -m taps.run',
        formatter_class=_ArgparseFormatter,
    )
    parser.add_argument(
        '--config',
        '-c',
        default=argparse.SUPPRESS,
        help='base toml configuration file to load',
    )

    group = parser.add_argument_group('app options')
    group.add_argument(
        '--app',
        choices=list(apps.keys()),
        dest='app.name',
        metavar='APP',
        help='app choice {%(choices)s}',
    )

    group = parser.add_argument_group('engine options')
    group.add_argument(
        '--engine.executor',
        '--executor',
        choices=list(plugins.get_executor_configs().keys()),
        default=argparse.SUPPRESS,
        dest='engine.executor.name',
        metavar='EXECUTOR',
        help='executor choice {%(choices)s} (default: process-pool)',
    )
    group.add_argument(
        '--engine.filter',
        '--filter',
        choices=list(plugins.get_filter_configs().keys()),
        default=argparse.SUPPRESS,
        dest='engine.filter.name',
        metavar='FILTER',
        help='filter choice {%(choices)s} (default: null)',
    )
    group.add_argument(
        '--engine.transformer',
        '--transformer',
        choices=list(plugins.get_transformer_configs().keys()),
        default=argparse.SUPPRESS,
        dest='engine.transformer.name',
        metavar='TRANSFORMER',
        help='transformer choice {%(choices)s} (default: null)',
    )

    # Strip --help from argv so we can quickly parse the base options
    # to figure out which config types we will need to use. --help
    # will be parsed again by CliSettingsSource.
    _argv = list(filter(lambda v: v not in ['-h', '--help'], argv))
    base_options = vars(parser.parse_known_args(_argv)[0])
    base_options = {k: v for k, v in base_options.items() if v is not None}
    config_file = base_options.pop('config', None)
    toml_options = _parse_toml_options(config_file)

    # base_options takes precedence over toml_options if there are
    # matching keys.
    base_options = {**toml_options, **base_options}
    if 'app.name' not in base_options or base_options['app.name'] is None:
        raise ValueError(
            'Missing the app name option. Either provides --app {APP} via '
            'the CLI args or add the app.name attribute the config file.',
        )

    settings_cls = _make_settings_cls(base_options)
    base_namespace = argparse.Namespace(**base_options)

    cli_settings = CliSettingsSource(
        settings_cls,
        cli_avoid_json=True,
        cli_parse_args=argv,
        cli_parse_none_str='null',
        cli_use_class_docs_for_groups=False,
        root_parser=parser,
        add_argument_method=_add_argument,
        add_argument_group_method=_add_argument_group,
        parse_args_method=functools.partial(
            argparse.ArgumentParser.parse_args,
            namespace=base_namespace,
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    return settings_cls(_cli_settings_source=cli_settings)


def _cwd_run_dir(
    func: Callable[[Config, pathlib.Path], None],
) -> Callable[[Config, pathlib.Path], None]:
    @functools.wraps(func)
    def _decorator(config: Config, run_dir: pathlib.Path) -> None:
        origin = pathlib.Path().absolute()
        try:
            os.chdir(run_dir)
            func(config, run_dir)
        finally:
            os.chdir(origin)

    return _decorator


@_cwd_run_dir
def run(config: Config, run_dir: pathlib.Path) -> None:
    """Run an application using the configuration.

    This function changes the current working directory to
    `config.run.run_dir` so that all paths are relative to the current
    working directory.
    """
    start = time.perf_counter()

    logger.log(RUN_LOG_LEVEL, f'Starting app (name={config.app.name})')
    logger.log(RUN_LOG_LEVEL, config)
    logger.log(RUN_LOG_LEVEL, f'Runtime directory: {run_dir}')

    config.write_toml('config.toml')

    app = config.app.get_app()
    engine = config.engine.get_engine()

    with contextlib.closing(app), engine:
        app.run(engine=engine, run_dir=run_dir)

    runtime = time.perf_counter() - start
    logger.log(
        RUN_LOG_LEVEL,
        f'Finished app (name={config.app.name}, '
        f'runtime={runtime:.2f}s, tasks={engine.tasks_executed})',
    )


def main(argv: Sequence[str] | None = None) -> int:  # noqa: D103
    argv = argv if argv is not None else sys.argv[1:]
    config = parse_args_to_config(argv)
    run_dir = make_run_dir(config)

    log_file = (
        None
        if config.logging.file_name is None
        else run_dir / config.logging.file_name
    )
    init_logging(
        log_file,
        config.logging.level,
        config.logging.file_level,
        force=True,
    )

    try:
        run(config, run_dir)
    except BaseException:
        logger.exception('Caught unhandled exception')
        return 1
    else:
        return 0
