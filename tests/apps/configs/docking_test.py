from __future__ import annotations

import pathlib
import sys
from unittest import mock

from taps.apps.configs.docking import DockingConfig


def test_create_app(tmp_path: pathlib.Path) -> None:
    config = DockingConfig(
        smi_file_name_ligand=tmp_path / 'test',
        receptor=tmp_path / 'receptor',
        tcl_path=tmp_path / 'test',
    )

    with mock.patch.dict(
        sys.modules,
        {'taps.apps.docking.app': mock.MagicMock()},
    ):
        config.get_app()
