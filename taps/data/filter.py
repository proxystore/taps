from __future__ import annotations

import math
import pickle
import sys
from typing import Any
from typing import Protocol
from typing import runtime_checkable


@runtime_checkable
class Filter(Protocol):
    """Filter protocol."""

    def __call__(self, obj: Any) -> bool:
        """Check if an abject passes through the filter."""
        ...


class NullFilter:
    """Null filter that lets all objects pass through."""

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        return True


class ObjectSizeFilter:
    """Object size filter.

    Checks if the size of an object (computed using
    [`sys.getsizeof()`][sys.getsizeof]) is greater than a minimum size and
    less than a maximum size.

    Warning:
        [`sys.getsizeof()`][sys.getsizeof] does not count the size of objects
        referred to by the main object.

    Args:
        min_bytes: Minimum size threshold (inclusive) to pass through the
            filter.
        max_bytes: Maximum size threshold (inclusive) to pass through the
            filter.
    """

    def __init__(
        self,
        *,
        min_bytes: int = 0,
        max_bytes: float = math.inf,
    ) -> None:
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        size = sys.getsizeof(obj)
        return self.min_bytes <= size <= self.max_bytes


class ObjectTypeFilter:
    """Object type filter.

    Checks if an object is of a certain type.

    Args:
        types: Types to check.
    """

    def __init__(self, *types: type) -> None:
        self.types = types

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        return isinstance(obj, self.types)


class PickleSizeFilter:
    """Object size filter.

    Checks if the size of an object (computed using size of the pickled object)
    is greater than a minimum size and less than a maximum size.

    Warning:
        Pickling large objects can take significant time.

    Args:
        min_bytes: Minimum size threshold (inclusive) to pass through the
            filter.
        max_bytes: Maximum size threshold (inclusive) to pass through the
            filter.
    """

    def __init__(
        self,
        *,
        min_bytes: int = 0,
        max_bytes: float = math.inf,
    ) -> None:
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        size = len(pickle.dumps(obj))
        return self.min_bytes <= size <= self.max_bytes
