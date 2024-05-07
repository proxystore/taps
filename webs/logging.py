from __future__ import annotations

import logging
import pathlib
import sys
from typing import Any

# Used by benchmark runners and harnesses
RUN_LOG_LEVEL = 22
# Use within workflows
WORK_LOG_LEVEL = 21


def init_logging(
    logfile: pathlib.Path | None = None,
    level: int | str = logging.INFO,
    logfile_level: int | str = logging.INFO,
    force: bool = False,
) -> None:
    """Initialize logging with custom formats.

    Adds a custom log levels RUN and WORK which are higher than INFO and
    lower than WARNING. RUN is used by the workflow benchmark harness
    and WORK is using within the workflows.

    Usage:
        >>> logger = init_logger(...)
        >>> logger.log(RUN_LOG_LEVEL, 'message')

    Args:
        logfile (str): option filepath to write log to (default: None).
        level (int, str): minimum logging level (default: INFO).
        logfile_level (int, str): minimum logging level for the logfile
            (default: INFO).
        force (bool): remove any existing handlers attached to the root
            handler. This option is useful to silencing the third-party
            package logging. Note: should not be set when running inside
            pytest (default: False).
    """
    logging.addLevelName(RUN_LOG_LEVEL, 'RUN')
    logging.addLevelName(WORK_LOG_LEVEL, 'WORK')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)

    handlers: list[logging.Handler] = [stdout_handler]
    if logfile is not None:
        logfile.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(logfile)
        handler.setLevel(logfile_level)
        handlers.append(handler)

    kwargs: dict[str, Any] = {}
    if force:  # pragma: no cover
        kwargs['force'] = force

    logging.basicConfig(
        format=(
            '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
            '%(message)s'
        ),
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        handlers=handlers,
        **kwargs,
    )
