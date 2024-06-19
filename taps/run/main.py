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

from pydantic_settings import CliSettingsSource

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


def _parse_args(
    parser: argparse.ArgumentParser,
    args: Sequence[str],
) -> argparse.Namespace:
    cli_args = parser.parse_args(args)

    if cli_args.config is not None:
        with open(cli_args.config, 'rb') as f:
            config_options = tomllib.load(f)
        toml_args = argparse.Namespace(**_flatten_dict(config_options))
    else:
        toml_args = argparse.Namespace()

    # cli_args takes precedence when the same key is present in both.
    final_args = {**vars(toml_args), **vars(cli_args)}
    return argparse.Namespace(**final_args)


def parse_args_to_config(argv: Sequence[str]) -> Config:
    """Parse sequence of string arguments into a config.

    Args:
        argv: Sequence of string arguments.

    Returns:
        Configuration.
    """
    parser = argparse.ArgumentParser(
        description='Task Performance Suite.',
        prog='python -m taps.run',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '-c',
        '--config',
        help='toml configuration file to load',
    )

    cli_settings = CliSettingsSource(
        Config,
        cli_avoid_json=True,
        cli_parse_args=argv,
        cli_parse_none_str='null',
        root_parser=parser,
        parse_args_method=_parse_args,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    return Config(_cli_settings_source=cli_settings)


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
