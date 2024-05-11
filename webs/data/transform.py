from __future__ import annotations

from typing import Any
from typing import Generic
from typing import Iterable
from typing import Mapping
from typing import NoReturn
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from webs.data.filter import Filter
from webs.data.filter import NullFilter

K = TypeVar('K')
T = TypeVar('T')
IdentifierT = TypeVar('IdentifierT')


@runtime_checkable
class Transformer(Protocol[IdentifierT]):
    """Object transformer protocol."""

    def is_identifier(self, obj: T) -> bool:
        """Check if the object is an identifier instance."""
        ...

    def transform(self, obj: T) -> IdentifierT:
        """Transform the object into an identifier.

        Args:
            obj: Object to transform.

        Returns:
            Identifier object that can be used to resolve `obj`.
        """
        ...

    def resolve(self, identifier: IdentifierT) -> Any:
        """Resolve an object from an identifier.

        Args:
            identifier: Identifier to an object.

        Returns:
            The resolved object.
        """
        ...


class NullTransformer:
    """Null transformer that does no transformations."""

    def is_identifier(self, obj: Any) -> bool:
        """Check if the object is an identifier instance.

        Always `False` in this implementation.
        """
        return False

    def transform(self, obj: T) -> T:
        """Transform the object into an identifier.

        Args:
            obj: Object to transform.

        Returns:
            Identifier object that can be usd to resolve `obj`.
        """
        return obj

    def resolve(self, identifier: Any) -> NoReturn:
        """Resolve an object from an identifier.

        Args:
            identifier: Identifier to an object.

        Returns:
            The resolved object.
        """
        raise NotImplementedError(
            f'{self.__class__.__name__} does not support identifiers',
        )


class TaskDataTransformer(Generic[IdentifierT]):
    """Task data transformer.

    This class combines a simple object
    [`Transformer`][webs.data.transform.Transformer] and a
    [`Filter`][webs.data.filter.Filter] into useful methods for transforming
    the positional arguments, keyword arguments, and results of tasks.

    Args:
        transformer: Object transformer.
        filter_: A filter which when called on an object returns `True` if
            the object should be transformed.
    """

    def __init__(
        self,
        transformer: Transformer[IdentifierT],
        filter_: Filter | None = None,
    ) -> None:
        self.transformer = transformer
        self.filter_ = NullFilter() if filter_ is None else filter_

    def transform(self, obj: T) -> T | IdentifierT:
        """Transform an object.

        Transforms `obj` into an identifier if it passes the filter check.
        The identifier can later be used to resolve the object.
        """
        if self.filter_(obj):
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
