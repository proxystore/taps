from __future__ import annotations

import contextlib
import functools
import logging
import os
import pathlib
import sys
import time
from typing import Callable
from typing import Sequence

from pydantic import ValidationError

import taps
from taps.logging import init_logging
from taps.logging import RUN_LOG_LEVEL
from taps.run.config import Config
from taps.run.config import make_run_dir
from taps.run.env import Environment
from taps.run.parse import parse_args_to_config
from taps.run.utils import prettify_mapping
from taps.run.utils import prettify_validation_error

logger = logging.getLogger('taps.run')


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


def _log_config(config: Config) -> None:
    logger.log(
        RUN_LOG_LEVEL,
        f'Configuration:\n{prettify_mapping(config.model_dump())}',
    )
    if config.version != taps.__version__:
        logger.warning(
            f'The configuration specifies TaPS version {config.version}, but '
            f'the current version of TaPS is {taps.__version__}. '
            'Application behavior can differ across versions',
        )


@_cwd_run_dir
def run(config: Config, run_dir: pathlib.Path) -> None:
    """Run an application using the configuration.

    This function changes the current working directory to
    `config.run.run_dir` so that all paths are relative to the current
    working directory.
    """
    start = time.perf_counter()

    logger.log(RUN_LOG_LEVEL, f'Starting app (name={config.app.name})')
    _log_config(config)
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

    try:
        config = parse_args_to_config(argv)
    except ValidationError as e:  # pragma: no cover
        raise prettify_validation_error(e, Config) from e

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

    logger.log(RUN_LOG_LEVEL, f'CLI Arguments: {" ".join(argv)}')
    logger.log(
        RUN_LOG_LEVEL,
        f'Environment:\n{Environment.collect().format()}',
    )

    try:
        run(config, run_dir)
    except BaseException:
        logger.exception('Caught unhandled exception')
        return 1
    else:
        return 0
