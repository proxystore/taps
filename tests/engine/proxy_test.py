from __future__ import annotations

import pathlib
from typing import TypeVar

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from proxystore.store.utils import get_key

from taps.engine import AppEngine
from taps.engine import TaskFuture

T = TypeVar('T')


def fake_factory() -> bytes:
    raise RuntimeError('Proxy was resolved!')


def identity(x: T) -> T:
    return x


@pytest.mark.parametrize(
    'executor_fixture',
    ('process_executor', 'dask_process_executor'),
)
def test_proxy_unintentional_resolves(
    executor_fixture: str,
    request,
) -> None:
    executor = request.getfixturevalue(executor_fixture)

    proxy = Proxy(fake_factory, cache_defaults=True, target=b'')

    with AppEngine(executor) as engine:
        task: TaskFuture[Proxy[bytes]] = engine.submit(identity, proxy)
        result = task.result()

        assert isinstance(result, Proxy)
        with pytest.raises(RuntimeError):
            len(result)


def check(x: Proxy[str], size: int) -> Proxy[str]:
    assert len(x) == size

    store = get_store(x)
    assert store is not None
    assert store.metrics is not None
    proxy_metrics = store.metrics.get_metrics(get_key(x))
    assert proxy_metrics is not None
    assert proxy_metrics.times['factory.resolve'].count == 1

    return x


@pytest.mark.parametrize(
    'executor_fixture',
    ('process_executor', 'dask_process_executor'),
)
def test_proxy_resolve_count(
    executor_fixture: str,
    tmp_path: pathlib.Path,
    request,
) -> None:
    executor = request.getfixturevalue(executor_fixture)

    with Store(
        'test-proxy-resolve-count',
        FileConnector(str(tmp_path)),
        metrics=True,
        populate_target=True,
        register=True,
    ) as store, AppEngine(executor) as engine:
        value = 'test-value'
        proxy = store.proxy(value)
        task = engine.submit(check, proxy, size=len(value))
        result = task.result()

        key = get_key(proxy)
        assert store.metrics is not None
        key_metrics = store.metrics.get_metrics(key)
        assert key_metrics is not None
        assert 'factory.resolve' not in key_metrics.times

        assert result == value
        key_metrics = store.metrics.get_metrics(key)
        assert key_metrics is not None
        assert key_metrics.times['factory.resolve'].count == 1
