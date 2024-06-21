# Montage Mosaic

This application is based on the [Parsl implementation](https://github.com/Parsl/parallel_patterns/blob/master/Montage%20Mosaic.ipynb) of the Montage Getting Started tutorial.

The Montage Mosaic application takes a directory of input astronomy images and stitches them into a single mosaic using Montage tools.

## Installation

This application requires additional package dependencies and for the Montage binaries to be installed.
We suggest using Conda so the correct compilers for Montage can be installed.
```bash
conda create --name taps-montage python=3.11
conda activate taps-montage
conda install -c conda-forge gcc=9.5 libnsl
pip install -e .[montage]
```

!!! warning

    Build instructions may be different depending on your system.
    Here we used Ubuntu 22.04.

Next, download, build, and install Montage, available at http://montage.ipac.caltech.edu/docs/download2.html.
We will do this inside of our Conda environment directory.
```bash
cd $CONDA_PREFIX
curl -L http://montage.ipac.caltech.edu/download/Montage_v6.0.tar.gz | tar -xz
cd Montage
make
export PATH="$CONDA_PREFIX/Montage/bin:$PATH"
```
You can now return to your TaPS directory.

## Data

Input data available at http://montage.ipac.caltech.edu/docs/Kimages.tar.
The following command will download and extract the tarfile to `data/Kimages`.
```bash
curl -L http://montage.ipac.caltech.edu/docs/Kimages.tar | tar -x -C data/
```

## Example

The application can be invoked using the downloaded data.

```bash
python -m taps.run --app montage --app.img-folder data/Kimages --engine.executor process-pool
```

!!! failure

    If you get an error like:
    ```
    montage_wrapper.status.MontageError: mProject: File (/home/cc/taps/data/Kimages/._aK_asky_990502s1350092.fits) is not a FITS image
    ```
    This is a side-effect of the tarfile being created on MacOS and MacOS using these special files for extra information.
    Simply remove those files from the image directory.
    ```bash
    rm -rf data/Kimages/._*
    ```

This will produce a single final FITS file within `data/` inside of the run directory.
