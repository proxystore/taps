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
from proxystore.store import Store
from pydantic import Field
from pydantic import field_validator

from webs.data.config import register
from webs.data.transform import TransformerConfig

T = TypeVar('T')


@register(name='proxy')
class ProxyFileTransformerConfig(TransformerConfig):
    """Proxy file transformer config."""

    ps_type: Literal['file', 'redis'] = Field(
        description='connector type (file or redis)',
    )
    ps_file_dir: Optional[str] = Field(  # noqa: UP007
        None,
        description='file connector cache directory',
    )
    ps_redis_addr: Optional[str] = Field(  # noqa: UP007
        None,
        description='redis connector server address',
    )
    ps_extract_target: bool = Field(
        False,
        description=(
            'extract the target from the proxy when resolving the identifier'
        ),
    )

    @field_validator('ps_file_dir', mode='before')
    @classmethod
    def _resolve_file_dir(cls, path: str) -> str:
        return str(pathlib.Path(path).resolve())

    def get_transformer(self) -> ProxyTransformer:
        """Create a transformer instance from the config."""
        connector: Connector[Any]
        if self.ps_type == 'file':
            if self.ps_file_dir is None:  # pragma: no cover
                raise ValueError(
                    'Option --ps-file-dir is required when --ps-type file.',
                )
            connector = FileConnector(self.ps_file_dir)
        elif self.ps_type == 'redis':
            if self.ps_redis_addr is None:
                raise ValueError(  # pragma: no cover
                    'Option --ps-redis-addr is required when --ps-type redis.',
                )
            parts = self.ps_redis_addr.split(':')
            host, port = parts[0], int(parts[1])
            connector = RedisConnector(host, port)
        else:
            raise AssertionError(
                f'Unknown proxy transformer type: {self.ps_type}.',
            )

        return ProxyTransformer(
            store=Store('transformer', connector),
            extract_target=self.ps_extract_target,
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
