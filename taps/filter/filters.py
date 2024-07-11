from __future__ import annotations

import math
import pickle
import re
import sys
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import Sequence


@runtime_checkable
class Filter(Protocol):
    """Filter protocol."""

    def __call__(self, obj: Any) -> bool:
        """Check if an abject passes through the filter."""
        ...


class AllFilter:
    """All filter that lets all objects pass through."""

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        return True


class NullFilter:
    """Null filter that lets no objects pass through."""

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        return False


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

    Checks if an object is of a certain type using [`isinstance()`][isinstance]
    or by pattern matching against the name of the type.

    Args:
        types: Types to check.
        patterns: Regex compatible patterns to compare against the name of the
            object's type.
    """

    def __init__(
        self,
        *types: type,
        patterns: Sequence[str] | None = None,
    ) -> None:
        self.types = types
        self.patterns = tuple(patterns if patterns is not None else [])

    def __call__(self, obj: Any) -> bool:
        """Check if an object passes through the filter."""
        if isinstance(obj, self.types):
            return True

        cls_name = type(obj).__name__
        for pattern in self.patterns:
            if re.search(pattern, cls_name) is not None:
                return True

        return False


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
