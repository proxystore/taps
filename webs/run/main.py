from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Sequence

# This import is necessary to ensure that all the workflow
# implementations get imported and therefore registered.
from webs import wf  # noqa: F401
from webs.executor.config import ExecutorChoicesConfig
from webs.executor.config import get_executor_config
from webs.executor.workflow import WorkflowExecutor
from webs.logging import init_logging
from webs.logging import RUN_LOG_LEVEL
from webs.run.config import BenchmarkConfig
from webs.run.config import RunConfig
from webs.workflow import get_registered

logger = logging.getLogger('webs.run')


def parse_args_to_config(argv: Sequence[str]) -> BenchmarkConfig:
    """Parse sequence of string arguments into a config.

    Args:
        argv: Sequence of string arguments.

    Returns:
        Configuration.
    """
    parser = argparse.ArgumentParser(
        description='Workflow benchmark suite.',
        usage='python -m webs.run',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    subparsers = parser.add_subparsers(title='Workflows', dest='name')

    workflows = get_registered()
    workflow_names = sorted(workflows.keys())
    for workflow_name in workflow_names:
        workflow = workflows[workflow_name]
        subparser = subparsers.add_parser(workflow.name)

        RunConfig.add_argument_group(subparser, argv=argv, required=True)
        ExecutorChoicesConfig.add_argument_group(
            subparser,
            argv=argv,
            required=True,
        )
        workflow.config_type.add_argument_group(
            subparser,
            argv=argv,
            required=True,
        )

    args = parser.parse_args(argv)
    options = vars(args)

    workflow_name = options['name']
    executor_config = get_executor_config(**options)
    run_config = RunConfig(**options)
    workflow_config = workflows[workflow_name].config_type(**options)

    return BenchmarkConfig(
        name=workflow_name,
        timestamp=datetime.now(),
        executor=executor_config,
        run=run_config,
        workflow=workflow_config,
    )


def run(config: BenchmarkConfig) -> None:
    """Run a workflow using the configuration."""
    start = time.perf_counter()
    logger.log(RUN_LOG_LEVEL, f'Starting workflow (name={config.name})')
    logger.log(RUN_LOG_LEVEL, config)

    compute_executor = config.executor.get_executor()
    workflow = get_registered()[config.name].from_config(config.workflow)

    with workflow, WorkflowExecutor(compute_executor) as executor:
        workflow.run(executor=executor, run_dir=config.get_run_dir())

    runtime = time.perf_counter() - start
    logger.log(
        RUN_LOG_LEVEL,
        f'Finished workflow (name={config.name}, runtime={runtime:.2f}s)',
    )


def main(argv: Sequence[str] | None = None) -> int:  # noqa: D103
    argv = argv if argv is not None else sys.argv[1:]
    config = parse_args_to_config(argv)

    init_logging(
        config.get_log_file(),
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
