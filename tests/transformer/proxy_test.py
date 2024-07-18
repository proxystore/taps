from __future__ import annotations

import pathlib
import pickle

import pytest
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from proxystore.store import unregister_store
from proxystore.store.config import ConnectorConfig

from taps.transformer import ProxyTransformer
from taps.transformer import ProxyTransformerConfig


def test_file_config(tmp_path: pathlib.Path) -> None:
    config = ProxyTransformerConfig(
        connector=ConnectorConfig(
            kind='file',
            options={'store_dir': str(tmp_path)},
        ),
    )
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
