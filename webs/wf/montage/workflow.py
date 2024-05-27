from __future__ import annotations

import logging
import pathlib
import sys

import pandas as pd

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.montage.config import MontageWorkflowConfig

logger = logging.getLogger(__name__)


def print_message(message: str) -> None:
    """Print a message."""
    logger.log(WORK_LOG_LEVEL, message)


def configure_montage(
    img_folder: pathlib.Path,
    img_tbl: pathlib.Path,
    img_hdr: pathlib.Path,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    """Montage Mosaic workflow setup.

    This function generates a header file bounding a
    collection of data specified by the input image dir.

    Args:
        img_folder (str): path to input image directory
        img_tbl (str): name of the image table file
        img_hdr (str): name of the image header file
        output_dir (str): output directory path

    Returns:
        pathlib.Path: the created output directory
    """
    import montage_wrapper as montage

    imgtbl_log = montage.mImgtbl(str(img_folder), str(img_tbl))

    logger.debug(imgtbl_log)

    mkhdr_log = montage.mMakeHdr(str(img_tbl), str(img_hdr))
    logger.debug(mkhdr_log)

    output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir


def mproject(
    input_fn: pathlib.Path,
    template: pathlib.Path,
    output_dir: pathlib.Path,
    output_name: pathlib.Path,
) -> pathlib.Path:
    """Wrapper to Montage mProject function.

    The function reprojects a single image to the scale defined
    in a FITS header template file.

    Args:
        input_fn (pathlib.Path): FITS file to reproject
        template (pathlib.Path): FITS header file used to define
            the desired output
        output_dir (pathlib.Path): output directory path
        output_name (pathlib.Path): output file basename

    Returns:
        pathlib.Path: output directory
    """
    import montage_wrapper as montage

    output = output_dir / output_name
    project_log = montage.mProject(str(input_fn), str(output), str(template))
    logger.debug(project_log)

    return output_dir  # helps establish dependencies


def mimgtbl(
    img_dir: pathlib.Path,
    tbl_name: str,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    """Wrapper to Montage Imgtbl function.

    The function extracts the FITS header geometry information
    from a set of files and creates an ASCII image metadata table.

    Args:
        img_dir (pathlib.Path): Input image directory
        tbl_name (str): FITS table name
        output_dir (pathlib.Path): Output directory to save hdr to

    Returns:
        pathlib.Path: created table file
    """
    import montage_wrapper as montage

    # It's the same path over and over so just grab the first occurrence
    tbl_file = output_dir / tbl_name
    imgtbl_log = montage.mImgtbl(str(img_dir), str(tbl_file))
    logger.debug(imgtbl_log)

    return tbl_file


def moverlaps(
    img_tbl: pathlib.Path,
    diffs_name: str,
    output_dir: pathlib.Path,
) -> tuple[pathlib.Path, pathlib.Path]:
    """Wrapper to Montage Overlaps function.

    The function takes a list of of images and generates a list of overlaps.

    Args:
        img_tbl (pathlib.Path): image metadata file
        diffs_name (str): output table name of overlapping areas
        output_dir (pathlib.Path): output directory path

    Returns:
        tuple[pathlib.Path, pathlib.Path]: directory containing
            overlap images and difference table
    """
    import montage_wrapper as montage

    diffs_tbl = output_dir / diffs_name
    overlaps_log = montage.mOverlaps(str(img_tbl), str(diffs_tbl))

    logger.debug(overlaps_log)
    diff_dir = output_dir / 'diffdir'
    diff_dir.mkdir(parents=True, exist_ok=True)
    return diff_dir, diffs_tbl


def mdiff(
    image_1: pathlib.Path,
    image_2: pathlib.Path,
    template: pathlib.Path,
    output_dir: pathlib.Path,
    output_name: str,
) -> pathlib.Path:
    """Wrapper to Montage diff function.

    The function subtracts one image from another
    (both in the same projection)

    Args:
        image_1 (pathlib.Path): first input file
            for differencing.
        image_2 (pathlib.Path): second input file
            for differencing.
        template (pathlib.Path): FITS header file used to
            define the desired output.
        output_dir (pathlib.Path): Output directory name.
        output_name (str): output difference file basename.

    Returns:
        pathlib.Path: output filepath
    """
    import montage_wrapper as montage

    diff_dir = output_dir / 'diffdir'
    diff_dir.mkdir(exist_ok=True, parents=True)
    output_file = diff_dir / output_name
    log_str = montage.mDiff(
        str(image_1),
        str(image_2),
        str(output_file),
        str(template),
    )
    logger.debug(log_str)

    return output_file


def bgexec_prep(
    img_table: pathlib.Path,
    diffs_table: pathlib.Path,
    diff_dir: pathlib.Path,
    output_dir: pathlib.Path,
) -> tuple[pathlib.Path, pathlib.Path]:
    """Prep to call Montage bg function.

    The function creates an image-to-image difference parameter table and
    then applies a set of corrections to achieve a best global fit.

    Args:
        img_table (pathlib.Path): reprojected image metadata list
        diffs_table (pathlib.Path): table file list of input difference images
        diff_dir (pathlib.Path): directory for temporary difference files
        output_dir (pathlib.Path): output directory path

    Returns:
        tuple[pathlib.Path, pathlib.Path]: correction dir path,
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
    logger.debug(fit_log)

    bg_log = montage.mBgModel(
        str(img_table),
        str(fits_tbl),
        str(corrections_tbl),
    )
    logger.info(bg_log)

    corrdir = output_dir / 'corrdir'
    corrdir.mkdir(parents=True, exist_ok=True)

    return corrdir, corrections_tbl


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
        in_image (pathlib.Path): Input FITS file
        out_image (pathlib.Path): Output background-removed FITS file
        a (float): A coefficient in (A*x + B*y + C) background level equation
        b (float): B coefficient in (A*x + B*y + C) background level equation
        c (float): C level in (A*x + B*y + C) background level equation

    Returns:
        pathlib.Path: output image path
    """
    import montage_wrapper as montage

    background_log = montage.mBackground(
        str(in_image),
        str(out_image),
        a,
        b,
        c,
    )
    logger.debug(background_log)

    return out_image


def madd(
    images_table: pathlib.Path,
    template_header: pathlib.Path,
    out_image: pathlib.Path,
    corr_dir: pathlib.Path,
) -> pathlib.Path:
    """Coadd reprojected images to form a mosaic.

    Args:
        images_table (pathlib.Path): table file containing metadata
            for images to be coadded.
        template_header (pathlib.Path): FITS header template to use
            in generation of output FITS.
        out_image (pathlib.Path): Output FITS image
        corr_dir (pathlib.Path): Directory containing reprojected image

    Returns:
        pathlib.Path: FITS output header file.
    """
    import montage_wrapper as montage

    add_log = montage.mAdd(
        str(images_table),
        str(template_header),
        str(out_image),
        str(corr_dir),
    )
    logger.debug(add_log)
    return out_image


class MontageWorkflow(ContextManagerAddIn):
    """Montage workflow.

    Args:
        img_folder (str): path to input image directory
        img_tbl (str): name of the image table file
        img_hdr (str): name of the image header file
        output_dir (str): output directory path
    """

    name = 'montage'
    config_type = MontageWorkflowConfig

    def __init__(
        self,
        img_folder: str,
        img_tbl: str,
        img_hdr: str,
        output_dir: str,
    ) -> None:
        self.img_folder = pathlib.Path(img_folder).absolute()
        self.img_tbl = pathlib.Path(img_tbl).absolute()
        self.img_hdr = pathlib.Path(img_hdr).absolute()
        self.output_dir = pathlib.Path(output_dir).absolute()
        super().__init__()

    @classmethod
    def from_config(cls, config: MontageWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(
            img_folder=config.img_folder,
            img_tbl=config.img_tbl,
            img_hdr=config.img_hdr,
            output_dir=config.output_dir,
        )

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        output_dir_fut = executor.submit(
            configure_montage,
            self.img_folder,
            self.img_tbl,
            self.img_hdr,
            self.output_dir,
        )

        mproject_outputs = []

        for image in self.img_folder.glob('*.fits'):
            input_image = self.img_folder / image
            output_image_name = f'hdu0_{image.name}'

            out = executor.submit(
                mproject,
                input_fn=input_image,
                template=self.img_hdr,
                output_dir=output_dir_fut,
                output_name=output_image_name,
            )
            mproject_outputs.append(out)

        _ = [i.result() for i in mproject_outputs]

        img_tbl_fut = executor.submit(
            mimgtbl,
            img_dir=self.output_dir,
            tbl_name='images.tbl',
            output_dir=output_dir_fut,
        )

        diffs_data_fut = executor.submit(
            moverlaps,
            img_tbl=img_tbl_fut,
            diffs_name='diffs.tbl',
            output_dir=output_dir_fut,
        )
        diffs_dir, diffs_tbl = diffs_data_fut.result()

        df = pd.read_csv(diffs_tbl, comment='#', sep='\\s+').drop(0)
        images1 = list(df['|.1'])
        images2 = list(df['cntr2'])
        outputs = list(df['|.2'])
        outputs_2 = []

        for i in range(len(images1)):
            image1 = self.output_dir / images1[i]
            image2 = self.output_dir / images2[i]

            out = executor.submit(
                mdiff,
                image_1=image1,
                image_2=image2,
                output_name=outputs[i],
                template=self.img_hdr,
                output_dir=output_dir_fut,
            )
            outputs_2.append(out)

        _ = [i.result() for i in outputs_2]

        fcorrdir = executor.submit(
            bgexec_prep,
            img_table=img_tbl_fut,
            diffs_table=diffs_tbl,
            diff_dir=diffs_dir,
            output_dir=self.output_dir,
        )

        corrdir, corrtbl = fcorrdir.result()

        corrections = pd.read_csv(
            corrtbl,
            comment='|',
            sep='\\s+',
        )
        corrections.loc[90] = list(corrections.columns)
        corrections.columns = ['id', 'a', 'b', 'c']

        # for i in range(len(corrections)):
        #    corrections.loc['id'][i] = int(corrections['id'][i])
        corrections['id'] = corrections['id'].astype(int)

        images_table = pd.read_csv(
            img_tbl_fut.result(),
            comment='|',
            sep='\\s+',
        )

        bgexec_outputs = []

        for i in range(len(images_table)):
            input_image = list(images_table['fitshdr'])[i]
            file_name = (list(images_table['fitshdr'])[i]).replace(
                str(self.output_dir) + '/',
                '',
            )
            output_image = corrdir / file_name
            correction_values = list(
                corrections.loc[corrections['id'] == i].values[0],
            )

            output_mb = executor.submit(
                mbackground,
                in_image=input_image,
                out_image=output_image,
                a=correction_values[1],
                b=correction_values[2],
                c=correction_values[3],
            )

            bgexec_outputs.append(output_mb)

        _ = [i.result() for i in bgexec_outputs]

        mosaic_out = self.output_dir / 'm17.fits'
        mosaic_future = executor.submit(
            madd,
            img_tbl_fut,
            self.img_hdr,
            mosaic_out,
            corrdir,
        )

        logger.info(f'Created output FITS file at {mosaic_future.result()}')
