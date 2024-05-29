from __future__ import annotations

import logging
import pathlib

from taps.logging import init_logging
from taps.logging import RUN_LOG_LEVEL


def test_logging_no_file() -> None:
    init_logging()

    logger = logging.getLogger()
    logger.log(RUN_LOG_LEVEL, 'test')


def test_logging_with_file(tmp_path: pathlib.Path) -> None:
    filepath = tmp_path / 'log.txt'

    init_logging(filepath)

    logger = logging.getLogger()
    logger.info('test')
