from __future__ import annotations

import os


def make_parent_dirs(filepath: str) -> None:
    """Make parent directories of a filepath."""
    parent_dir = os.path.dirname(filepath)
    if len(parent_dir) > 0 and not os.path.isdir(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
