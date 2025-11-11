from __future__ import annotations

import logging
import pathlib

import pandas as pd

from taps.engine import Engine
from taps.engine import task
from taps.engine import wait
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)


def configure_montage(
    img_folder: pathlib.Path,
    img_tbl: pathlib.Path,
    img_hdr: pathlib.Path,
) -> None:
    """Montage Mosaic application setup.

    This function generates a header file bounding a
    collection of data specified by the input image dir.

    Args:
        img_folder: Path to input image directory.
        img_tbl: Name of the image table file.
        img_hdr: Name of the image header file.
    """
    import montage_wrapper as montage

    imgtbl_log = montage.mImgtbl(str(img_folder), str(img_tbl))
    logger.debug(f'mImgtbl:\n{imgtbl_log}')

    mkhdr_log = montage.mMakeHdr(str(img_tbl), str(img_hdr))
    logger.debug(f'mMakeHdr:\n{mkhdr_log}')


@task()
def mproject(
    input_path: pathlib.Path,
    template_path: pathlib.Path,
    output_path: pathlib.Path,
) -> pathlib.Path:
    """Wrapper to Montage mProject function.

    The function reprojects a single image to the scale defined
    in a FITS header template file.

    Args:
        input_path: FITS file to reproject.
        template_path: FITS header file used to define the desired output.
        output_path: Output filepath.

    Returns:
        Output filepath for chaining task dependencies.
    """
    import montage_wrapper as montage

    project_log = montage.mProject(
        str(input_path),
        str(output_path),
        str(template_path),
    )
    logger.debug(f'mProject:\n{project_log}')

    return output_path


@task()
def mimgtbl(
    img_dir: pathlib.Path,
    tbl_path: pathlib.Path,
) -> pathlib.Path:
    """Wrapper to Montage Imgtbl function.

    The function extracts the FITS header geometry information
    from a set of files and creates an ASCII image metadata table.

    Args:
        img_dir: Input image directory.
        tbl_path: FITS table path.

    Returns:
        Path to table file for chaining task dependencies.
    """
    import montage_wrapper as montage

    imgtbl_log = montage.mImgtbl(str(img_dir), str(tbl_path))
    logger.debug(f'mImgtbl:\n{imgtbl_log}')

    return tbl_path


@task()
def moverlaps(
    img_tbl: pathlib.Path,
    diffs_tbl: pathlib.Path,
) -> pathlib.Path:
    """Wrapper to Montage Overlaps function.

    The function takes a list of of images and generates a list of overlaps.

    Args:
        img_tbl: Image metadata file.
        diffs_tbl: Path for the output diff table.

    Returns:
        Path to the difference table file for chaining task dependencies.
    """
    import montage_wrapper as montage

    overlaps_log = montage.mOverlaps(str(img_tbl), str(diffs_tbl))
    logger.debug(f'mOverlaps:\n{overlaps_log}')

    return diffs_tbl


@task()
def mdiff(
    image_1: pathlib.Path,
    image_2: pathlib.Path,
    template: pathlib.Path,
    output_path: pathlib.Path,
) -> pathlib.Path:
    """Wrapper to Montage diff function.

    The function subtracts one image from another (both in the same
    projection).

    Args:
        image_1: First input file for differencing.
        image_2: Second input file for differencing.
        template: FITS header file used to define the desired output.
        output_path: Output filepath.

    Returns:
        Output filepath for chaining task dependencies.
    """
    import montage_wrapper as montage

    diff_log = montage.mDiff(
        str(image_1),
        str(image_2),
        str(output_path),
        str(template),
    )
    logger.debug(f'mDiff:\n{diff_log}')

    return output_path


@task()
def bgexec_prep(
    img_table: pathlib.Path,
    diffs_table: pathlib.Path,
    diff_dir: pathlib.Path,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    """Prep to call Montage bg function.

    The function creates an image-to-image difference parameter table and
    then applies a set of corrections to achieve a best global fit.

    Args:
        img_table: Reprojected image metadata list.
        diffs_table: Table file list of input difference images.
        diff_dir: Directory for temporary difference files.
        output_dir: Output directory path.

    Returns:
            corrections table path
    """
    import montage_wrapper as montage

    fits_tbl = output_dir / 'fits.tbl'
    corrections_tbl = output_dir / 'corrections.tbl'

    fit_log = montage.mFitExec(
        str(diffs_table),
        str(fits_tbl),
        str(diff_dir),
    )
    logger.debug(f'mFitExec\n{fit_log}')

    bg_log = montage.mBgModel(
        str(img_table),
        str(fits_tbl),
        str(corrections_tbl),
    )
    logger.debug(f'mBgModel\n{bg_log}')

    return corrections_tbl


@task()
def mbackground(
    in_image: pathlib.Path,
    out_image: pathlib.Path,
    a: float,
    b: float,
    c: float,
) -> pathlib.Path:
    """Wrapper to Montage Background function.

    Function subtracts a planar background from a FITS image.

    Args:
        in_image: Input FITS file.
        out_image: Output background-removed FITS file.
        a: A coefficient in (A*x + B*y + C) background level equation.
        b: B coefficient in (A*x + B*y + C) background level equation.
        c: C level in (A*x + B*y + C) background level equation.

    Returns:
        Output image path for chaining task dependencies.
    """
    import montage_wrapper as montage

    bg_log = montage.mBackground(str(in_image), str(out_image), a, b, c)
    logger.debug(f'mBackground:\n{bg_log}')

    return out_image


@task()
def madd(
    images_table: pathlib.Path,
    template_header: pathlib.Path,
    out_image: pathlib.Path,
    corr_dir: pathlib.Path,
) -> pathlib.Path:
    """Coadd reprojected images to form a mosaic.

    Args:
        images_table: table file containing metadata for images to be coadded.
        template_header: FITS header template to use in generation of output
            FITS.
        out_image: Output FITS image.
        corr_dir: Directory containing reprojected image.

    Returns:
        FITS output header file.
    """
    import montage_wrapper as montage

    add_log = montage.mAdd(
        str(images_table),
        str(template_header),
        str(out_image),
        str(corr_dir),
    )
    logger.debug(f'mAdd:\n{add_log}')

    return out_image


class MontageApp:
    """Montage application.

    Args:
        img_folder: Path to input image directory.
        img_tbl: Name of the image table file.
        img_hdr: Name of the image header file.
        output_dir: Output directory path for intermediate and result data.
    """

    def __init__(
        self,
        img_folder: pathlib.Path,
        img_tbl: str = 'Kimages.tbl',
        img_hdr: str = 'Kimages.hdr',
        output_dir: str = 'data/',
    ) -> None:
        self.img_folder = img_folder
        self.img_tbl = img_tbl
        self.img_hdr = img_hdr
        self.output_dir = output_dir

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:  # noqa: PLR0915
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        output_dir = run_dir / self.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        img_tbl = output_dir / self.img_tbl
        img_hdr = output_dir / self.img_hdr
        configure_montage(self.img_folder, img_tbl, img_hdr)

        logger.log(
            APP_LOG_LEVEL,
            f'Configured output directory ({output_dir})',
        )

        projections_dir = output_dir / 'projections'
        projections_dir.mkdir(parents=True, exist_ok=True)

        mproject_outputs = []
        logger.log(APP_LOG_LEVEL, 'Starting projections')
        for image in self.img_folder.glob('*.fits'):
            input_image = self.img_folder / image
            output_image_path = projections_dir / f'hdu0_{image.name}'

            out = engine.submit(
                mproject,
                input_path=input_image,
                template_path=img_hdr,
                output_path=output_image_path,
            )
            mproject_outputs.append(out)

        wait(mproject_outputs)
        logger.log(APP_LOG_LEVEL, 'Projections completed')

        img_tbl_fut = engine.submit(
            mimgtbl,
            img_dir=projections_dir,
            tbl_path=output_dir / 'images.tbl',
        )
        diffs_tbl_fut = engine.submit(
            moverlaps,
            img_tbl=img_tbl_fut,
            diffs_tbl=output_dir / 'diffs.tbl',
        )
        diffs_dir = output_dir / 'diffs'
        diffs_dir.mkdir(parents=True, exist_ok=True)

        diffs_tbl = diffs_tbl_fut.result()
        df = pd.read_csv(diffs_tbl, comment='#', sep='\\s+').drop(0)
        images1 = list(df['|.1'])
        images2 = list(df['cntr2'])
        outputs = list(df['|.2'])

        mdiff_futures = []
        logger.log(APP_LOG_LEVEL, 'Starting difference computations')
        for image1, image2, output in zip(
            images1,
            images2,
            outputs,
            strict=False,
        ):
            future = engine.submit(
                mdiff,
                image_1=projections_dir / image1,
                image_2=projections_dir / image2,
                template=img_hdr,
                output_path=diffs_dir / output,
            )
            mdiff_futures.append(future)

        wait(mdiff_futures)
        logger.log(APP_LOG_LEVEL, 'Differences completed')

        corrections_fut = engine.submit(
            bgexec_prep,
            img_table=img_tbl_fut,
            diffs_table=diffs_tbl,
            diff_dir=diffs_dir,
            output_dir=output_dir,
        )

        corrections_dir = output_dir / 'corrections'
        corrections_dir.mkdir(parents=True, exist_ok=True)

        corrections_tbl = corrections_fut.result()

        corrections = pd.read_csv(corrections_tbl, comment='|', sep='\\s+')
        corrections.loc[90] = list(corrections.columns)
        corrections.columns = ['id', 'a', 'b', 'c']
        corrections['id'] = corrections['id'].astype(int)

        img_tbl = img_tbl_fut.result()
        images_table = pd.read_csv(img_tbl, comment='|', sep='\\s+')

        bgexec_futures = []
        logger.log(APP_LOG_LEVEL, 'Starting background computations')
        for i, input_image in enumerate(list(images_table['fitshdr'])):
            input_path = pathlib.Path(input_image)
            output_path = corrections_dir / input_path.name
            correction_values = list(
                corrections.loc[corrections['id'] == i].values[0],
            )

            future = engine.submit(
                mbackground,
                in_image=input_path,
                out_image=output_path,
                a=correction_values[1],
                b=correction_values[2],
                c=correction_values[3],
            )

            bgexec_futures.append(future)

        wait(bgexec_futures)
        logger.log(APP_LOG_LEVEL, 'Backgrounds completed')

        mosaic_future = engine.submit(
            madd,
            img_tbl_fut,
            img_hdr,
            output_dir / 'm17.fits',
            corrections_dir,
        )

        logger.log(
            APP_LOG_LEVEL,
            f'Created output FITS file at {mosaic_future.result()}',
        )
