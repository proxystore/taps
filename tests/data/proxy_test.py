from __future__ import annotations

import pickle
from unittest import mock

import pytest
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from proxystore.store import unregister_store

from taps.data.proxy import ProxyFileTransformerConfig
from taps.data.proxy import ProxyTransformer


def test_file_config() -> None:
    config = ProxyFileTransformerConfig(
        ps_type='file',
        ps_file_dir='test',
        ps_redis_addr='localhost:0',
    )
    with mock.patch('taps.data.proxy.FileConnector'):
        transformer = config.get_transformer()
        transformer.close()


def test_redis_config() -> None:
    config = ProxyFileTransformerConfig(
        ps_type='redis',
        ps_file_dir='test',
        ps_redis_addr='localhost:0',
    )
    with mock.patch('taps.data.proxy.RedisConnector'):
        transformer = config.get_transformer()
        transformer.close()


@pytest.mark.parametrize('extract', (True, False))
def test_proxy_transformer(extract: bool) -> None:
    with Store(
        'test-proxy-transformer',
        LocalConnector(),
        register=True,
    ) as store:
        transformer = ProxyTransformer(store, extract_target=extract)

        obj = [1, 2, 3]
        identifier = transformer.transform(obj)
        assert transformer.is_identifier(identifier)
        resolved = transformer.resolve(identifier)
        assert isinstance(resolved, Proxy) != extract
        assert resolved == obj

        transformer.close()


def test_proxy_transformer_pickling() -> None:
    name = 'test-proxy-transformer-pickle'
    with Store(name, LocalConnector(), register=True) as store:
        transformer = ProxyTransformer(store)
        pickled = pickle.dumps(transformer)
        transformer = pickle.loads(pickled)

        unregister_store(name)
        transformer = pickle.loads(pickled)
        assert get_store(name) is not None

        transformer.close()
