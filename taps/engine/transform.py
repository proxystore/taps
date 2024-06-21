from __future__ import annotations

from concurrent.futures import Future
from typing import Any
from typing import Generic
from typing import Iterable
from typing import Mapping
from typing import TypeVar

from taps.filter import Filter
from taps.transformer import Transformer

K = TypeVar('K')
T = TypeVar('T')
IdentifierT = TypeVar('IdentifierT')


class TaskTransformer(Generic[IdentifierT]):
    """Task data transformer.

    This class combines a simple object
    [`Transformer`][taps.transformer.Transformer] and a
    [`Filter`][taps.filter.Filter] into useful methods for transforming
    the positional arguments, keyword arguments, and results of tasks.

    Args:
        transformer: Object transformer.
        filter_: A filter which when called on an object returns `True` if
            the object should be transformed.
    """

    def __init__(
        self,
        transformer: Transformer[IdentifierT],
        filter_: Filter,
    ) -> None:
        self.transformer = transformer
        self.filter_ = filter_

    def close(self) -> None:
        """Close the transformer."""
        self.transformer.close()

    def transform(self, obj: T) -> T | IdentifierT:
        """Transform an object.

        Transforms `obj` into an identifier if it passes the filter check.
        The identifier can later be used to resolve the object.
        """
        if self.filter_(obj) and not isinstance(obj, Future):
            return self.transformer.transform(obj)
        else:
            return obj

    def transform_iterable(
        self,
        iterable: Iterable[T],
    ) -> tuple[T | IdentifierT, ...]:
        """Transform each object in an iterable."""
        return tuple(self.transform(obj) for obj in iterable)

    def transform_mapping(self, mapping: Mapping[K, T]) -> dict[K, Any]:
        """Transform each value in a mapping."""
        return {k: self.transform(v) for k, v in mapping.items()}

    def resolve(self, obj: Any) -> Any:
        """Resolve an object.

        Resolves the object if it is an identifier, otherwise returns the
        passed object.
        """
        if self.transformer.is_identifier(obj):
            return self.transformer.resolve(obj)
        else:
            return obj

    def resolve_iterable(self, iterable: Iterable[Any]) -> tuple[Any, ...]:
        """Resolve each object in an iterable."""
        return tuple(self.resolve(obj) for obj in iterable)

    def resolve_mapping(self, mapping: Mapping[K, Any]) -> dict[K, Any]:
        """Resolve each value in a mapping."""
        return {k: self.resolve(v) for k, v in mapping.items()}
