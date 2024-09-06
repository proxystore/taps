from __future__ import annotations

import json
import pathlib

from taps.record import JSONRecordLogger
from taps.record import NullRecordLogger


def test_json_record_logger(tmp_path: pathlib.Path) -> None:
    dict_a = {'a': 1, 'b': 2}
    dict_b = {'c': '3', 'd': [4, 5]}

    logfile = tmp_path / 'log.json'
    with JSONRecordLogger(logfile) as logger:
        assert isinstance(repr(logger), str)

        logger.log(dict_a)
        logger.log(dict_b)

    with open(logfile) as f:
        line_a, line_b = f.readlines()

    assert json.loads(line_a) == dict_a
    assert json.loads(line_b) == dict_b


def test_null_record_logger() -> None:
    with NullRecordLogger() as logger:
        logger.log({})
