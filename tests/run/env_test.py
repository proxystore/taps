from __future__ import annotations

from taps.run.env import Environment


def test_collect_environment() -> None:
    env = Environment.collect()
    assert isinstance(env.format(), str)
