from __future__ import annotations

import pathlib
from typing import Any
from typing import Literal
from typing import Optional
from typing import TypeVar

from proxystore.connectors.file import FileConnector
from proxystore.connectors.protocols import Connector
from proxystore.connectors.redis import RedisConnector
from proxystore.proxy import extract
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from pydantic import Field
from pydantic import field_validator

from taps.plugins import register
from taps.transformer.config import DataTransformerConfig

T = TypeVar('T')


@register('transformer')
class ProxyTransformerConfig(DataTransformerConfig):
    """Proxy transformer configuration."""

    name: Literal['proxy'] = Field(
        'proxy',
        description='name of transformer type',
    )
    connector: Literal['file', 'redis'] = Field(
        description='connector type (file or redis)',
    )
    file_dir: Optional[str] = Field(  # noqa: UP007
        None,
        description='file connector cache directory',
    )
    redis_addr: Optional[str] = Field(  # noqa: UP007
        None,
        description='redis connector server address',
    )
    extract_target: bool = Field(
        False,
        description=(
            'extract the target from the proxy when resolving the identifier'
        ),
    )

    @field_validator('file_dir', mode='before')
    @classmethod
    def _resolve_file_dir(cls, path: str) -> str:
        return str(pathlib.Path(path).resolve())

    def get_transformer(self) -> ProxyTransformer:
        """Create a transformer from the configuration."""
        connector: Connector[Any]
        if self.connector == 'file':
            if self.file_dir is None:  # pragma: no cover
                raise ValueError(
                    'Option file_dir is required for the file connector.',
                )
            connector = FileConnector(self.file_dir)
        elif self.connector == 'redis':
            if self.redis_addr is None:
                raise ValueError(  # pragma: no cover
                    'Option redis_addr is required for the Redis connector.',
                )
            parts = self.redis_addr.split(':')
            host, port = parts[0], int(parts[1])
            connector = RedisConnector(host, port)
        else:
            raise AssertionError(
                f'Unknown ProxyStore connector type: {self.connector}.',
            )

        return ProxyTransformer(
            store=Store(
                'transformer',
                connector=connector,
                register=True,
                populate_target=True,
            ),
            extract_target=self.extract_target,
        )


class ProxyTransformer:
    """Proxy object transformer.

    Transforms objects into proxies which act as the identifier.

    Args:
        store: Store instance to use for proxying objects.
        extract_target: When `True`, resolving an identifier (i.e., a proxy)
            will return the target object. Otherwise, the proxy is returned
            since a proxy can act as the target object.
    """

    def __init__(
        self,
        store: Store[Any],
        *,
        extract_target: bool = False,
    ) -> None:
        self.store = store
        self.extract_target = extract_target

    def __getstate__(self) -> dict[str, Any]:
        return {
            'config': self.store.config(),
            'extract_target': self.extract_target,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        store = get_store(state['config']['name'])
        if store is not None:
            self.store = store
        else:
            self.store = Store.from_config(state['config'])
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
        return extract(identifier) if self.extract_target else identifier
