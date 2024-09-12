from __future__ import annotations

import pathlib
import sys
from unittest import mock

import pytest
from pydantic import ValidationError

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


def test_validate_config(tmp_path: pathlib.Path) -> None:
    with pytest.raises(
        ValidationError,
        match='Number of initial simulations',
    ):
        DockingConfig(
            smi_file_name_ligand=tmp_path / 'test',
            receptor=tmp_path / 'receptor',
            tcl_path=tmp_path / 'test',
            initial_simulations=1,
        )
