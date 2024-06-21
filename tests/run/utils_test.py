from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from taps.run.utils import flatten_mapping
from taps.run.utils import prettify_validation_error
from testing.app import MockAppConfig


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


@pytest.mark.parametrize('pass_model', (True, False))
def test_prettify_validation_error(pass_model: bool) -> None:
    try:
        MockAppConfig(tasks='abc', extra=True)  # type: ignore[call-arg]
    except ValidationError as e:
        if pass_model:
            error = prettify_validation_error(e, MockAppConfig)
        else:
            error = prettify_validation_error(e)

    assert isinstance(error, ValueError)
    assert 'extra_forbidden' in str(error)
    assert 'int_parsing' in str(error)
