from __future__ import annotations

from typing import Any
from typing import TypeVar

from proxystore.proxy import extract
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.store.utils import get_key

T = TypeVar('T')


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
        obj: T | Proxy[T]
        if self.extract_target:
            obj = extract(identifier)
            self.store.evict(get_key(identifier))
        else:
            obj = identifier
        return obj
