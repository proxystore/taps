from __future__ import annotations

import abc
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from taps.config import Config

K = TypeVar('K')
T = TypeVar('T')
IdentifierT = TypeVar('IdentifierT')


class DataTransformerConfig(Config, abc.ABC):
    """Data transformer configuration abstract base class."""

    @abc.abstractmethod
    def get_transformer(self) -> DataTransformer[Any]:
        """Create a transformer instance from the config."""
        ...


@runtime_checkable
class DataTransformer(Protocol[IdentifierT]):
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
