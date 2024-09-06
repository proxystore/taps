from __future__ import annotations

import logging
import pathlib
import sys
from typing import Any

# Used by benchmark runners and harnesses
RUN_LOG_LEVEL = 22
# Used within applications
APP_LOG_LEVEL = 21
# Used within framework
TRACE_LOG_LEVEL = 5


def get_repr(obj: Any) -> str:
    """Nice object `repr` for logging.

    Args:
        obj: Object to get representation of.

    Returns:
        String representation of `obj`.
    """
    if type(obj).__repr__ is not object.__repr__:
        # https://stackoverflow.com/a/19628560
        return repr(obj)
    else:
        return type(obj).__name__


def init_logging(
    logfile: pathlib.Path | None = None,
    level: int | str = logging.INFO,
    logfile_level: int | str | None = None,
    force: bool = False,
) -> None:
    """Initialize logging with custom formats.

    Adds a custom log levels `RUN` and `APP` which are higher than `INFO` and
    lower than `WARNING`. `RUN` is used by the benchmark harness
    and `APP` is using within the applications. Also adds the `TRACE` level
    which is lower than `DEBUG` and used within the framework.

    Usage:
        ```python
        import logging
        from taps.logging import init_logger

        init_logger(...)

        logger = logging.getLogger(__name__)
        logger.log(RUN_LOG_LEVEL, 'message')
        ```

    Args:
        logfile: Optional filepath to write log to.
        level: Minimum logging level.
        logfile_level: Minimum logging level for the logfile. If `None`,
            defaults to the value of `level`.
        force: Remove any existing handlers attached to the root
            handler. This option is useful to silencing the third-party
            package logging. Note: should not be set when running inside
            pytest.
    """
    logging.addLevelName(RUN_LOG_LEVEL, 'RUN')
    logging.addLevelName(APP_LOG_LEVEL, 'APP')
    logging.addLevelName(TRACE_LOG_LEVEL, 'TRACE')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)

    logfile_level = logfile_level if logfile_level is not None else level

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
            '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) > '
            '%(message)s'
        ),
        datefmt='%Y-%m-%d %H:%M:%S',
        level=TRACE_LOG_LEVEL,
        handlers=handlers,
        **kwargs,
    )

    # This needs to be after the configuration of the root logger because
    # warnings get logged to a 'py.warnings' logger.
    logging.captureWarnings(True)
