from __future__ import annotations

import math
import pickle
import re
import sys
from typing import Any
from typing import List
from typing import Literal
from typing import Optional
from typing import Sequence

from pydantic import Field

from taps.filter._protocol import Filter
from taps.filter._protocol import FilterConfig
from taps.plugins import register


class ObjectSizeFilter:
    """Object size filter.

    Checks if the size of an object (computed using
    [`sys.getsizeof()`][sys.getsizeof]) is greater than a minimum size and
    less than a maximum size.

    Warning:
        [`sys.getsizeof()`][sys.getsizeof] does not count the size of objects
        referred to by the main object.

    Example:
        ```python
        from taps.filter import ObjectSizeFilter

        filter_ = ObjectSizeFilter(min_bytes=100)
        assert not filter_('small')
        assert filter_('large' * 100)
        ```

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
        size = sys.getsizeof(obj)
        return self.min_bytes <= size <= self.max_bytes


@register('filter')
class ObjectSizeFilterConfig(FilterConfig):
    """[`ObjectSizeFilter`][taps.filter.ObjectSizeFilter] plugin configuration."""  # noqa: E501

    name: Literal['object-size'] = Field(
        'object-size',
        description='name of filter type',
    )
    min_size: int = Field(0, description='minimum object size in bytes')
    max_size: float = Field(
        math.inf,
        description='maximum object size in bytes',
    )

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return ObjectSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )


class ObjectTypeFilter:
    """Object type filter.

    Checks if an object is of a certain type using [`isinstance()`][isinstance]
    or by pattern matching against the name of the type.

    Example:
        ```python
        from taps.filter import ObjectTypeFilter

        filter_ = ObjectTypeFilter(int, str)
        assert filter_(42)
        assert filter_('value')
        assert not filter_(3.14)
        ```

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
        if isinstance(obj, self.types):
            return True

        cls_name = type(obj).__name__
        for pattern in self.patterns:
            if re.search(pattern, cls_name) is not None:
                return True

        return False


@register('filter')
class ObjectTypeFilterConfig(FilterConfig):
    """[`ObjectTypeFilter`][taps.filter.ObjectTypeFilter] plugin configuration."""  # noqa: E501

    name: Literal['object-type'] = Field(
        'object-type',
        description='name of filter type',
    )
    patterns: Optional[List[str]] = Field(  # noqa: UP006,UP007
        None,
        description='list of patterns to match against type names',
    )

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return ObjectTypeFilter(patterns=self.patterns)


class PickleSizeFilter:
    """Object size filter.

    Checks if the size of an object (computed using size of the pickled object)
    is greater than a minimum size and less than a maximum size.

    Warning:
        Pickling large objects can take significant time, so this filter
        type is only recommended when the data transformation cost (e.g.,
        communication or storage) is significantly greater than serialization
        of the objects.

    Example:
        ```python
        from taps.filter import PickleSizeFilter

        filter_ = PickleSizeFilter(min_bytes=100)
        assert not filter_('small')
        assert filter_('large' * 100)
        ```

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
        size = len(pickle.dumps(obj))
        return self.min_bytes <= size <= self.max_bytes


@register('filter')
class PickleSizeFilterConfig(FilterConfig):
    """[`PickleSizeFilter`][taps.filter.PickleSizeFilter] plugin configuration."""  # noqa: E501

    name: Literal['pickle-size'] = Field(
        'pickle-size',
        description='name of filter type',
    )
    min_size: int = Field(0, description='minimum object size in bytes')
    max_size: float = Field(
        math.inf,
        description='maximum object size in bytes',
    )

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return PickleSizeFilter(
            min_bytes=self.min_size,
            max_bytes=self.max_size,
        )
