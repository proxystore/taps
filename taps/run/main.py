from __future__ import annotations

import contextlib
import functools
import logging
import pathlib
import sys
from typing import Callable
from typing import Sequence

from proxystore.utils.timer import Timer
from pydantic import ValidationError

import taps
from taps.logging import init_logging
from taps.logging import RUN_LOG_LEVEL
from taps.run.config import Config
from taps.run.config import make_run_dir
from taps.run.env import Environment
from taps.run.parse import parse_args_to_config
from taps.run.utils import change_cwd
from taps.run.utils import prettify_mapping
from taps.run.utils import prettify_validation_error
from taps.run.utils import update_environment

logger = logging.getLogger('taps.run')

CONFIG_FILENAME = 'config.toml'


def _cwd_run_dir(
    func: Callable[[Config, pathlib.Path], None],
) -> Callable[[Config, pathlib.Path], None]:
    @functools.wraps(func)
    def _decorator(config: Config, run_dir: pathlib.Path) -> None:
        with change_cwd(run_dir):
            logger.debug(f'Changed working directory to {run_dir}')
            return func(config, run_dir)

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

    This helper method (1) logs and writes the configuration to the run
    directory, (2), configures the benchmark app, (3) creates the engine,
    (4) runs the application, and (5) cleans up all resources afterwards.

    Note:
        This function changes the current working directory to
        `config.run.run_dir` so that all paths are relative to the current
        working directory.

    Args:
        config: Benchmark configuration.
        run_dir: Run directory to use.
    """
    timer = Timer()
    timer.start()

    logger.log(RUN_LOG_LEVEL, 'Starting benchmark...')
    _log_config(config)
    logger.log(RUN_LOG_LEVEL, f'Runtime directory: {run_dir}')

    config.write_toml(CONFIG_FILENAME)
    logger.debug(f'Wrote config to {CONFIG_FILENAME}')

    env_vars = config.run.env_vars if config.run.env_vars is not None else {}
    with update_environment(env_vars):
        with Timer() as app_init_timer:
            app = config.app.get_app()
        logger.log(
            RUN_LOG_LEVEL,
            f'Initialized app (name={config.app.name}, '
            f'type={type(app).__name__}, '
            f'elapsed={app_init_timer.elapsed_s:.3f}s)',
        )

        with Timer() as engine_init_timer:
            engine = config.engine.get_engine()
        logger.log(
            RUN_LOG_LEVEL,
            f'Initialized engine (elapsed={engine_init_timer.elapsed_s:.3f}s)',
        )
        logger.debug(repr(engine))

        with contextlib.closing(app), engine:
            logger.log(RUN_LOG_LEVEL, 'Running app...')
            with Timer() as app_timer:
                app.run(engine=engine, run_dir=run_dir)
            logger.log(
                RUN_LOG_LEVEL,
                f'Finished app (elapsed={app_timer.elapsed_s:.3f}s)',
            )

    timer.stop()
    logger.log(
        RUN_LOG_LEVEL,
        f'Finished benchmark (app={config.app.name}, '
        f'elapsed={timer.elapsed_s:.3f}s, tasks={engine.tasks_executed})',
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
