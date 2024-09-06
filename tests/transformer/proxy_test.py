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
from pydantic import ValidationError

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


def test_config_validation_error(tmp_path: pathlib.Path) -> None:
    with pytest.raises(
        ValidationError,
        match='Options async_resolve and extract_target cannot be enabled',
    ):
        ProxyTransformerConfig(
            connector=ConnectorConfig(
                kind='file',
                options={'store_dir': str(tmp_path)},
            ),
            async_resolve=True,
            extract_target=True,
        )


@pytest.mark.parametrize(
    ('extract', 'async_'),
    (
        (False, False),
        (True, False),
        (False, True),
    ),
)
def test_proxy_transformer(extract: bool, async_: bool) -> None:
    with Store(
        'test-proxy-transformer',
        LocalConnector(),
        register=True,
    ) as store:
        transformer = ProxyTransformer(
            store,
            async_resolve=async_,
            extract_target=extract,
        )
        assert isinstance(repr(transformer), str)

        obj = [1, 2, 3]
        identifier = transformer.transform(obj)
        assert transformer.is_identifier(identifier)
        resolved = transformer.resolve(identifier)
        assert isinstance(resolved, Proxy) != extract
        assert resolved == obj

        transformer.close()


def test_proxy_transformer_value_error() -> None:
    with Store(
        'test-proxy-transformer-value-error',
        LocalConnector(),
        register=True,
    ) as store:
        with pytest.raises(
            ValueError,
            match='Options async_resolve and extract_target cannot be enabled',
        ):
            ProxyTransformer(
                store,
                async_resolve=True,
                extract_target=True,
            )


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
