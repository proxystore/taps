from __future__ import annotations

import sys
from typing import Any
from typing import Literal
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import extract
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from proxystore.store.config import ConnectorConfig
from proxystore.store.utils import resolve_async
from pydantic import Field
from pydantic import model_validator

from taps.plugins import register
from taps.transformer._protocol import TransformerConfig

T = TypeVar('T')


@register('transformer')
class ProxyTransformerConfig(TransformerConfig):
    """[`ProxyTransformer`][taps.transformer.ProxyTransformer] plugin configuration."""  # noqa: E501

    name: Literal['proxystore'] = Field(
        'proxystore',
        description='Transformer name.',
    )
    connector: ConnectorConfig = Field(
        description='Connector configuration.',
    )
    cache_size: int = Field(16, description='cache size')
    async_resolve: bool = Field(
        False,
        description=(
            'Asynchronously resolve proxies. Not compatible with '
            'extract_target=True.'
        ),
    )
    extract_target: bool = Field(
        False,
        description=(
            'Extract the target from the proxy when resolving the identifier. '
            'Not compatible with async_resolve=True.'
        ),
    )
    populate_target: bool = Field(
        True,
        description='Populate target objects of newly created proxies.',
    )

    @model_validator(mode='after')
    def _validate_mutex_options(self) -> Self:
        if self.async_resolve and self.extract_target:
            raise ValueError(
                'Options async_resolve and extract_target cannot be '
                'enabled at the same time.',
            )
        return self

    def get_transformer(self) -> ProxyTransformer:
        """Create a transformer from the configuration."""
        connector = self.connector.get_connector()
        return ProxyTransformer(
            store=Store(
                'proxy-transformer',
                connector=connector,
                cache_size=self.cache_size,
                populate_target=self.populate_target,
                register=True,
            ),
            async_resolve=self.async_resolve,
            extract_target=self.extract_target,
        )


class ProxyTransformer:
    """Proxy object transformer.

    Transforms objects into proxies which act as the identifier.

    Args:
        store: Store instance to use for proxying objects.
        async_resolve: Begin asynchronously resolving proxies when the
            transformer resolves a proxy (which is otherwise a no-op unless
            `extract_target=True`). Not compatible with `extract_target=True`.
        extract_target: When `True`, resolving an identifier (i.e., a proxy)
            will return the target object. Otherwise, the proxy is returned
            since a proxy can act as the target object. Not compatible
            with `async_resolve=True`.
    """

    def __init__(
        self,
        store: Store[Any],
        *,
        async_resolve: bool = False,
        extract_target: bool = False,
    ) -> None:
        if async_resolve and extract_target:
            raise ValueError(
                'Options async_resolve and extract_target cannot be '
                'enabled at the same time.',
            )

        self.store = store
        self.async_resolve = async_resolve
        self.extract_target = extract_target

    def __repr__(self) -> str:
        ctype = type(self).__name__
        store = f'store={self.store}'
        async_ = f'async_resolve={self.async_resolve}'
        extract = f'extract_target={self.extract_target}'
        return f'{ctype}({store}, {async_}, {extract})'

    def __getstate__(self) -> dict[str, Any]:
        return {
            'config': self.store.config(),
            'async_resolve': self.async_resolve,
            'extract_target': self.extract_target,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        store = get_store(state['config'].name)
        if store is not None:
            self.store = store
        else:
            self.store = Store.from_config(state['config'])
        self.async_resolve = state['async_resolve']
        self.extract_target = state['extract_target']

    def close(self) -> None:
        """Close the transformer."""
        self.store.close()

    def is_identifier(self, obj: Any) -> bool:
        """Check if the object is an identifier instance."""
        return isinstance(obj, Proxy)

    def transform(self, obj: T) -> Proxy[T]:
        """Transform the object into an identifier.

        Args:
            obj: Object to transform.

        Returns:
            Identifier object that can be used to resolve `obj`.
        """
        return self.store.proxy(obj)

    def resolve(self, identifier: Proxy[T]) -> T | Proxy[T]:
        """Resolve an object from an identifier.

        Args:
            identifier: Identifier to an object.

        Returns:
            The resolved object or a proxy of the resolved object depending \
            on the setting of `extract_target`.
        """
        if self.extract_target:
            return extract(identifier)
        if self.async_resolve:
            resolve_async(identifier)
        return identifier
