"""Data download CLI for the docking app."""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Sequence

import requests

REPO = 'https://raw.githubusercontent.com/Parsl/parsl-docking-tutorial/1460cb2d79c4660cfc7144c394606fd101e272e6'
FILES = {
    '1iep_receptor.pdbqt': f'{REPO}/1iep_receptor.pdbqt',
    'dataset_orz_original_1k.csv': f'{REPO}/dataset_orz_original_1k.csv',
    'set_element.tcl': f'{REPO}/set_element.tcl',
}


def download(directory: pathlib.Path | str) -> None:
    """Download docking app files."""
    directory = pathlib.Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for name, source in FILES.items():
        output = directory / name
        with open(output, 'wb') as f:
            content = requests.get(source, stream=True).content
            f.write(content)
        print(f'Downloaded {output}')


def main(argv: Sequence[str] | None = None) -> int:
    """Data download CLI."""
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        description='Protein docking data downloader',
    )
    parser.add_argument(
        '-o',
        '--output',
        required=True,
        help='Output directory for downloaded files',
    )
    args = parser.parse_args(argv)

    download(args.output)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
