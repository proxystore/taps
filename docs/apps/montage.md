# Montage Mosaic

This workflow is based on the [Parsl implementation](https://github.com/Parsl/parallel_patterns/blob/master/Montage%20Mosaic.ipynb) of the Montage Getting Started tutorial.
The workflow takes a directory of inputs and uses Montage tools to create a mosaic of them.

Montage binaries must be pre-installed prior to execution.
Binaries are available on Homebrew (MacOS) or http://montage.ipac.caltech.edu/docs/download2.html

Input data available here: http://montage.ipac.caltech.edu/docs/Kimages.tar

## Example

```bash
python -m taps.run montage \
    --img-folder ${PWD}/Kimages \
    --img-tbl Kimages.tbl \
    --img-hdr Ktemplate.hdr \
    --output-dir Kprojdir \
    --executor process-pool \
    --max-processes 10
```
