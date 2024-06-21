from __future__ import annotations

from typing import Any

import pytest

from taps.run.utils import flatten_mapping


@pytest.mark.parametrize(
    ('given', 'expected'),
    (
        ({}, {}),
        ({'a': 1}, {'a': 1}),
        ({'a': {'b': 1}}, {'a.b': 1}),
        ({'a': {'b': {'c': 1}}, 'd': 2}, {'a.b.c': 1, 'd': 2}),
    ),
)
def test_flatten_mapping(
    given: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    result = flatten_mapping(given)
    assert result == expected
