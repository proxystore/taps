from __future__ import annotations

from taps.run.apps.registry import get_registered_apps


def test_get_registered_apps() -> None:
    get_registered_apps()
