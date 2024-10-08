from __future__ import annotations

import pathlib
import pickle
import shutil
import uuid
from typing import Any
from typing import Literal
from typing import NamedTuple
from typing import TypeVar

from pydantic import Field
from pydantic import field_validator

from taps.plugins import register
from taps.transformer._protocol import TransformerConfig

T = TypeVar('T')


@register('transformer')
class PickleFileTransformerConfig(TransformerConfig):
    """[`PickleFileTransformer`][taps.transformer.PickleFileTransformer] plugin configuration."""  # noqa: E501

    name: Literal['file'] = Field(
        'file',
        description='Transformer name.',
    )
    file_dir: str = Field(description='Object file directory.')

    def get_transformer(self) -> PickleFileTransformer:
        """Create a transformer from the configuration."""
        return PickleFileTransformer(self.file_dir)

    @field_validator('file_dir', mode='before')
    @classmethod
    def _resolve_file_dir(cls, path: str) -> str:
        return str(pathlib.Path(path).resolve())


class PickleFileIdentifier(NamedTuple):
    """Identifier type for the [`PickleFileTransformer`][taps.transformer.PickleFileTransformer].

    Attributes:
        cache_dir: Object directory.
        obj_id: Object ID.
    """  # noqa: E501

    cache_dir: pathlib.Path
    obj_id: uuid.UUID

    def path(self) -> pathlib.Path:
        """Get path to the object."""
        return self.cache_dir / str(self.obj_id)


class PickleFileTransformer:
    """Pickle file object transformer.

    Transforms objects by pickling the object and writing the pickled object
    to a file.

    Args:
        cache_dir: Directory to store pickled objects in.
    """

    def __init__(
        self,
        cache_dir: pathlib.Path | str,
    ) -> None:
        self.cache_dir = pathlib.Path(cache_dir).resolve()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(cache_dir='{self.cache_dir}')"

    def close(self) -> None:
        """Close the transformer."""
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def is_identifier(self, obj: Any) -> bool:
        """Check if the object is an identifier instance."""
        return isinstance(obj, PickleFileIdentifier)

    def transform(self, obj: T) -> PickleFileIdentifier:
        """Transform the object into an identifier.

        Args:
            obj: Object to transform.

        Returns:
            Identifier object that can be used to resolve `obj`.
        """
        identifier = PickleFileIdentifier(self.cache_dir, uuid.uuid4())
        filepath = identifier.path()
        filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, 'wb', buffering=0) as f:
            pickle.dump(obj, f)

        return identifier

    def resolve(self, identifier: PickleFileIdentifier) -> Any:
        """Resolve an object from an identifier.

        Args:
            identifier: Identifier to an object.

        Returns:
            The resolved object.
        """
        filepath = identifier.path()
        with open(filepath, 'rb') as f:
            obj = pickle.load(f)
        return obj
