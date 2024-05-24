from __future__ import annotations

from unittest import mock

import pytest
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import Store

from webs.data.proxy import ProxyFileTransformerConfig
from webs.data.proxy import ProxyTransformer


def test_file_config() -> None:
    config = ProxyFileTransformerConfig(
        ps_type='file',
        ps_file_dir='test',
        ps_redis_addr='localhost:0',
    )
    with mock.patch('webs.data.proxy.FileConnector'):
        config.get_transformer()


def test_redis_config() -> None:
    config = ProxyFileTransformerConfig(
        ps_type='redis',
        ps_file_dir='test',
        ps_redis_addr='localhost:0',
    )
    with mock.patch('webs.data.proxy.RedisConnector'):
        config.get_transformer()


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
