from __future__ import annotations

from typing import Any
from typing import Literal
from typing import NoReturn
from typing import TypeVar

from pydantic import Field

from taps.plugins import register
from taps.transformer._protocol import TransformerConfig

T = TypeVar('T')


@register('transformer')
class NullTransformerConfig(TransformerConfig):
    """[`NullTransformer`][taps.transformer.NullTransformer] plugin configuration."""  # noqa: E501

    name: Literal['null'] = Field(
        'null',
        description='name of transformer type',
    )

    def get_transformer(self) -> NullTransformer:
        """Create a transformer from the configuration."""
        return NullTransformer()


class NullTransformer:
    """No-op transformer.

    Rather than transforming an object, this transformer just returns
    the input object as its own identifier.
    """

    def close(self) -> None:
        """Close the transformer."""
        pass

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
