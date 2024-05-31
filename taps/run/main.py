from __future__ import annotations

import argparse
import collections
import contextlib
import functools
import logging
import os
import pathlib
import sys
import time
from datetime import datetime
from typing import Callable
from typing import Sequence

from taps.data.config import DataTransformerChoicesConfig

# This import is necessary to ensure that all the workflow
# implementations get imported and therefore registered.
from taps.data.config import FilterConfig
from taps.data.config import get_transformer_config
from taps.engine import AppEngine
from taps.executor.config import ExecutorChoicesConfig
from taps.executor.config import get_executor_config
from taps.logging import init_logging
from taps.logging import RUN_LOG_LEVEL
from taps.record import JSONRecordLogger
from taps.run.apps.registry import get_registered_apps
from taps.run.config import BenchmarkConfig
from taps.run.config import RunConfig

logger = logging.getLogger('taps.run')


def parse_args_to_config(argv: Sequence[str]) -> BenchmarkConfig:
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

    subparsers = parser.add_subparsers(
        title='Applications',
        dest='name',
        required=True,
        help='application to execute',
    )

    apps = collections.OrderedDict(sorted(get_registered_apps().items()))
    for name, config in apps.items():
        subparser = subparsers.add_parser(
            name,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        RunConfig.add_argument_group(subparser, argv=argv, required=True)
        ExecutorChoicesConfig.add_argument_group(
            subparser,
            argv=argv,
            required=True,
        )
        DataTransformerChoicesConfig.add_argument_group(
            subparser,
            argv=argv,
            required=False,
        )
        FilterConfig.add_argument_group(
            subparser,
            argv=argv,
            required=True,
        )
        config.add_argument_group(subparser, argv=argv, required=True)

    args = parser.parse_args(argv)
    options = vars(args)

    app_name = options['name']
    executor_config = get_executor_config(**options)
    transformer_config = get_transformer_config(**options)
    filter_config = FilterConfig(**options)
    run_config = RunConfig(**options)
    app_config = apps[app_name](**options)

    return BenchmarkConfig(
        name=app_name,
        timestamp=datetime.now(),
        app=app_config,
        executor=executor_config,
        transformer=transformer_config,
        filter=filter_config,
        run=run_config,
    )


def _cwd_run_dir(
    function: Callable[[BenchmarkConfig], None],
) -> Callable[[BenchmarkConfig], None]:
    @functools.wraps(function)
    def _decorator(config: BenchmarkConfig) -> None:
        origin = pathlib.Path().absolute()
        try:
            os.chdir(config.get_run_dir())
            function(config)
        finally:
            os.chdir(origin)

    return _decorator


@_cwd_run_dir
def run(config: BenchmarkConfig) -> None:
    """Run an application using the configuration.

    This function changes the current working directory to
    `config.run.run_dir` so that all paths are relative to the current
    working directory.
    """
    start = time.perf_counter()

    cwd = pathlib.Path.cwd().resolve()

    logger.log(RUN_LOG_LEVEL, f'Starting app (name={config.name})')
    logger.log(RUN_LOG_LEVEL, config)
    logger.log(RUN_LOG_LEVEL, f'Runtime directory: {cwd}')

    config_json = config.model_dump_json(exclude={'timestamp'}, indent=4)
    with open('config.json', 'w') as f:
        f.write(config_json)

    app = config.app.create_app()
    executor = config.executor.get_executor()
    data_transformer = config.transformer.get_transformer()
    data_filter = config.filter.get_filter()
    record_logger = JSONRecordLogger(config.run.task_record_file_name)
    engine = AppEngine(
        executor,
        data_transformer=data_transformer,
        data_filter=data_filter,
        record_logger=record_logger,
    )

    with contextlib.closing(app), record_logger, engine:
        app.run(engine=engine, run_dir=cwd)

    runtime = time.perf_counter() - start
    logger.log(
        RUN_LOG_LEVEL,
        f'Finished app (name={config.name}, '
        f'runtime={runtime:.2f}s, tasks={engine.tasks_executed})',
    )


def main(argv: Sequence[str] | None = None) -> int:  # noqa: D103
    argv = argv if argv is not None else sys.argv[1:]
    config = parse_args_to_config(argv)

    log_file = (
        None
        if config.run.log_file_name is None
        else config.get_run_dir() / config.run.log_file_name
    )
    init_logging(
        log_file,
        config.run.log_level,
        config.run.log_file_level,
        force=True,
    )

    try:
        run(config)
    except BaseException:
        logger.exception('Caught unhandled exception')
        return 1
    else:
        return 0
