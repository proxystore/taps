from __future__ import annotations

import logging
import sys
from types import TracebackType
from typing import Any
from typing import Generic
from typing import Iterable
from typing import Mapping

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
    from typing import TypeVar
else:  # pragma: <3.11 cover
    from typing_extensions import TypeVar

from taps.filter import Filter
from taps.future import is_future
from taps.logging import get_repr
from taps.logging import TRACE_LOG_LEVEL
from taps.transformer import Transformer

logger = logging.getLogger(__name__)

K = TypeVar('K')
T = TypeVar('T')
IdentifierT = TypeVar('IdentifierT', default=None)


class TaskTransformer(Generic[IdentifierT]):
    """Task data transformer.

    This class combines a simple object
    [`Transformer`][taps.transformer.Transformer] and a
    [`Filter`][taps.filter.Filter] into useful methods for transforming
    the positional arguments, keyword arguments, and results of tasks.

    Args:
        transformer: Object transformer. If `None`, no objects will be
            transformed.
        filter_: A filter which when called on an object returns `True` if
            the object should be transformed. If `None`, all objects will
            be transformed.
    """

    def __init__(
        self,
        transformer: Transformer[IdentifierT] | None = None,
        filter_: Filter | None = None,
    ) -> None:
        self.transformer = transformer
        self.filter_ = filter_

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f'TaskTransformer(transformer={get_repr(self.transformer)}, '
            f'filter={get_repr(self.filter_)})'
        )

    def close(self) -> None:
        """Close the transformer."""
        if self.transformer is not None:
            self.transformer.close()

    def transform(self, obj: T) -> T | IdentifierT:
        """Transform an object.

        Transforms `obj` into an identifier if it passes the filter check.
        The identifier can later be used to resolve the object.
        """
        filtered = self.filter_ is None or self.filter_(obj)
        if filtered and self.transformer is not None and not is_future(obj):
            identifier = self.transformer.transform(obj)
            logger.log(
                TRACE_LOG_LEVEL,
                f'Transformed object (type={type(obj).__name__}) into '
                f'identifier (type={type(identifier).__name__})',
            )
            return identifier
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
        if self.transformer is not None and self.transformer.is_identifier(
            obj,
        ):
            result = self.transformer.resolve(obj)
            logger.log(
                TRACE_LOG_LEVEL,
                f'Resolved identifier (type={type(obj).__name__}) into '
                f'object (type={type(result).__name__})',
            )
            return result
        else:
            return obj

    def resolve_iterable(self, iterable: Iterable[Any]) -> tuple[Any, ...]:
        """Resolve each object in an iterable."""
        return tuple(self.resolve(obj) for obj in iterable)

    def resolve_mapping(self, mapping: Mapping[K, Any]) -> dict[K, Any]:
        """Resolve each value in a mapping."""
        return {k: self.resolve(v) for k, v in mapping.items()}
