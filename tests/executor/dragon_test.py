from __future__ import annotations

import sys

import pytest

from taps.executor.dragon import DragonConfig
from taps.executor.utils import FutureDependencyExecutor


def test_dragon_config() -> None:
    DragonConfig(max_processes=1)


@pytest.mark.skipif(
    sys.platform != 'linux',
    reason='Dragon is only available on linux',
)
def test_dragon_executor() -> None:  # pragma: linux cover
    config = DragonConfig(max_processes=2)
    with config.get_executor() as executor:
        assert isinstance(executor, FutureDependencyExecutor)
        future = executor.submit(sum, [1, 2, 3], start=-6)
        assert future.result() == 0
