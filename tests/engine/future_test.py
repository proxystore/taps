from __future__ import annotations

import uuid
from concurrent.futures import Future
from unittest import mock

from taps.engine.future import is_future


def test_is_future() -> None:
    assert is_future(Future())
    assert not is_future('not-a-future')

    with mock.patch('dask.distributed.Future', autospec=True):
        from dask.distributed import Future as DaskFuture

        assert is_future(DaskFuture(uuid.uuid4()))
