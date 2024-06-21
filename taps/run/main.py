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

from taps.logging import init_logging
from taps.logging import RUN_LOG_LEVEL
from taps.run.config import Config
from taps.run.config import make_run_dir
from taps.run.parse import parse_args_to_config

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
