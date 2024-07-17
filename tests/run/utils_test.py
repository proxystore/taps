from __future__ import annotations

import os
import pathlib
import uuid
from typing import Any

import pytest
from pydantic import ValidationError

from taps.run.utils import change_cwd
from taps.run.utils import flatten_mapping
from taps.run.utils import prettify_mapping
from taps.run.utils import prettify_validation_error
from taps.run.utils import update_environment
from testing.app import MockAppConfig


def test_change_cwd(tmp_path: pathlib.Path) -> None:
    origin = pathlib.Path.cwd()
    with change_cwd(tmp_path) as yielded:
        assert pathlib.Path.cwd() == tmp_path == yielded
        assert pathlib.Path.cwd() != origin
    assert pathlib.Path.cwd() == origin


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


def test_prettify_mapping() -> None:
    data = {'a': {'c': [1, 2, 3], 'name': 'foo'}, 'c': 'baz', 'b': 'bar'}
    expected = """\
a:
  name: 'foo'
  c: [1, 2, 3]
b: 'bar'
c: 'baz'\
"""
    assert prettify_mapping(data) == expected


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


def test_update_environment() -> None:
    name = str(uuid.uuid4())
    value = str(uuid.uuid4())

    with update_environment({name: value}):
        assert name in os.environ
        assert os.environ[name] == value


def test_update_environment_restore() -> None:
    name = str(uuid.uuid4())
    initial_value = str(uuid.uuid4())
    temp_value = str(uuid.uuid4())

    os.environ[name] = initial_value
    with update_environment({name: temp_value}):
        assert os.environ[name] == temp_value
    assert os.environ[name] == initial_value
