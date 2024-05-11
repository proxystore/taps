from __future__ import annotations

import pytest
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import Store

from webs.data.proxy import ProxyTransformer


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
