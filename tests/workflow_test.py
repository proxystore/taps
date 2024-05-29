from __future__ import annotations

import uuid
from unittest import mock

import pytest

import taps
from taps.workflow import get_registered_workflow
from taps.workflow import get_registered_workflow_names


def test_get_registered_workflow_names() -> None:
    assert all(
        [isinstance(name, str) for name in get_registered_workflow_names()],
    )


def test_get_registered_workflow_key_error() -> None:
    name = str(uuid.uuid4())
    with pytest.raises(KeyError, match=name):
        get_registered_workflow(name)


def test_get_registered_workflow_import_error() -> None:
    name = str(uuid.uuid4())
    with mock.patch.dict(
        taps.workflow.REGISTERED_WORKFLOWS,
        {name: f'testing.workflow.{name}'},
    ):
        with pytest.raises(ImportError, match=name):
            get_registered_workflow(name)
