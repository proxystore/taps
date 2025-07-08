# Note: due to the external binary and data dependencies of the montage_wrapper
# package, these tests are *heavily* mocked. They essentially just test that
# the code is executable.
from __future__ import annotations

import contextlib
import pathlib
import sys
from typing import Generator
from unittest import mock

import pytest

from taps.apps.montage import bgexec_prep
from taps.apps.montage import configure_montage
from taps.apps.montage import madd
from taps.apps.montage import mbackground
from taps.apps.montage import mdiff
from taps.apps.montage import mimgtbl
from taps.apps.montage import MontageApp
from taps.apps.montage import moverlaps
from taps.apps.montage import mproject
from taps.engine import Engine


@pytest.fixture(autouse=True, scope='module')
def _mock_montage() -> Generator[None, None, None]:
    functions = [
        'mImgtbl',
        'mMakeHdr',
        'mProject',
        'mOverlaps',
        'mDiff',
        'mFitExec',
        'mBgModel',
        'mBackground',
    ]

    mods = {'montage_wrapper': mock.MagicMock()}
    with mock.patch.dict(sys.modules, mods), contextlib.ExitStack() as stack:
        for function in functions:
            stack.enter_context(mock.patch(f'montage_wrapper.{function}'))

        yield


def test_configure_montage_spec(tmp_path: pathlib.Path) -> None:
    configure_montage(
        img_folder=tmp_path / 'img_folder',
        img_tbl=tmp_path / 'img_tbl',
        img_hdr=tmp_path / 'img_hdr',
    )


def test_mproject_spec(tmp_path: pathlib.Path) -> None:
    mproject(
        input_path=tmp_path / 'input_path',
        template_path=tmp_path / 'template_path',
        output_path=tmp_path / 'output_path',
    )


def test_mimgtbl_spec(tmp_path: pathlib.Path) -> None:
    mimgtbl(img_dir=tmp_path / 'img_dir', tbl_path=tmp_path / 'tbl_path')


def test_moverlaps_spec(tmp_path: pathlib.Path) -> None:
    moverlaps(img_tbl=tmp_path / 'img_tbl', diffs_tbl=tmp_path / 'diffs_tbl')


def test_mdiff_spec(tmp_path: pathlib.Path) -> None:
    mdiff(
        image_1=tmp_path / 'image_1',
        image_2=tmp_path / 'image_2',
        template=tmp_path / 'template',
        output_path=tmp_path / 'output_path',
    )


def test_bgexec_prep_spec(tmp_path: pathlib.Path) -> None:
    bgexec_prep(
        img_table=tmp_path / 'img_table',
        diffs_table=tmp_path / 'diffs_table',
        diff_dir=tmp_path / 'diff_dir',
        output_dir=tmp_path / 'output_dir',
    )


def test_mbackground_spec(tmp_path: pathlib.Path) -> None:
    mbackground(
        in_image=tmp_path / 'in_image',
        out_image=tmp_path / 'out_image',
        a=1,
        b=1,
        c=1,
    )


def test_madd_spec(tmp_path: pathlib.Path) -> None:
    madd(
        images_table=tmp_path / 'images_table',
        template_header=tmp_path / 'template_header',
        out_image=tmp_path / 'out_image',
        corr_dir=tmp_path / 'corr_dir',
    )


def test_montage_app(engine: Engine, tmp_path: pathlib.Path) -> None:
    img_folder = tmp_path / 'images'
    img_folder.mkdir()
    for i in range(3):
        (img_folder / f'{i}.fits').touch()

    app = MontageApp(img_folder)

    diffs_tbl = tmp_path / 'diffs_tbl.csv'
    with open(diffs_tbl, 'w') as f:
        f.write("""\
| cntr1 | cntr2 | plus | minus | diff |
| int   | int   | char |  char | char |
 0 1 image1.fits image2.fits diff.fits
""")

    corrections_tbl = tmp_path / 'corrections.csv'
    with open(corrections_tbl, 'w') as f:
        f.write("""\
| id | a | b | c |
   0   0   0   0
   1   0   0   0
   4   0   0   0
""")

    img_tbl = tmp_path / 'images.csv'
    with open(img_tbl, 'w') as f:
        f.write("""\
fitshdr
image.fits
""")

    with (
        mock.patch(
            'taps.apps.montage.moverlaps',
            autospec=True,
            return_value=diffs_tbl,
        ),
        mock.patch(
            'taps.apps.montage.bgexec_prep',
            autospec=True,
            return_value=corrections_tbl,
        ),
        mock.patch(
            'taps.apps.montage.mimgtbl',
            autospec=True,
            return_value=img_tbl,
        ),
    ):
        app.run(engine, tmp_path)

    app.close()
