from __future__ import annotations

import abc
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

K = TypeVar('K')
T = TypeVar('T')
IdentifierT = TypeVar('IdentifierT')


@runtime_checkable
class Transformer(Protocol[IdentifierT]):
    """Data transformer protocol.

    A data transformer is used by the [`Engine`][taps.engine.Engine] to
    transform task parameters and results into alternative formats that are
    more suitable for communication.

    An object can be transformed using
    [`transform()`][taps.transformer.Transformer.transform] which returns
    an identifier. The identifier can then be provided to
    [`resolve()`][taps.transformer.Transformer.resolve], the inverse of
    [`transform()`][taps.transformer.Transformer.transform], which returns
    the original object.

    Data transformer implementations can implement object identifiers in any
    manner, provided identifiers are serializable. For example, a simple
    identifier could be a UUID corresponding to a database entry containing
    the serialized object.
    """

    def close(self) -> None:
        """Close the transformer.

        The transformer is only closed by the client once the application
        has finished executing (or raised an exception).
        """
        ...

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


class TransformerConfig(BaseModel, abc.ABC):
    """Abstract [`Transformer`][taps.transformer.Transformer] plugin configuration."""  # noqa: E501

    name: str = Field(description='Transformer name.')

    model_config: ConfigDict = ConfigDict(  # type: ignore[misc]
        extra='ignore',
        validate_default=True,
        validate_return=True,
    )

    @abc.abstractmethod
    def get_transformer(self) -> Transformer[Any]:
        """Create a transformer from the configuration."""
        ...
