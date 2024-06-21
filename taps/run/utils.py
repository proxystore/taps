from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any


def flatten_mapping(
    mapping: MutableMapping[str, Any],
    parent_key: str = '',
    separator: str = '.',
) -> dict[str, Any]:
    """Flatten the keys of nested mappings/dicts.

    Args:
        mapping: Nested mapping to flatten.
        parent_key: Prefix of parent keys when this function is called
            recursively.
        separator: Separator between key parts.

    Returns:
        Flattened dictionary.
    """
    items: list[tuple[str, Any]] = []
    for key, value in mapping.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(
                flatten_mapping(value, new_key, separator=separator).items(),
            )
        else:
            items.append((new_key, value))
    return dict(items)
