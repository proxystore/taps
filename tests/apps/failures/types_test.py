from __future__ import annotations

import pytest

from taps.apps.failures.types import FAILURE_FUNCTIONS
from taps.apps.failures.types import FailureType


def test_random_failure() -> None:
    for _ in range(10):
        failure = FailureType.random()
        assert isinstance(failure, FailureType)
        assert failure is not FailureType.RANDOM


@pytest.mark.parametrize(
    ('failure_type', 'exc_type'),
    (
        (FailureType.FAILURE, Exception),
        (FailureType.IMPORT, ImportError),
        (FailureType.ZERO_DIVISION, ZeroDivisionError),
    ),
)
def test_simple_failures(
    failure_type: FailureType,
    exc_type: type[Exception],
) -> None:
    function = FAILURE_FUNCTIONS[failure_type]
    with pytest.raises(exc_type, match=r'Failure injection error.'):
        function()
