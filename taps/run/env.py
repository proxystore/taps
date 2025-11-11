from __future__ import annotations

import dataclasses
import importlib
import json
import logging
import pathlib
import platform
import sys
from dataclasses import field
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import psutil

logger = logging.getLogger(__name__)


def _get_version(package: str) -> str:
    try:
        module = importlib.import_module(package)
    except ImportError:
        return 'not installed'

    try:
        return module.__version__
    except AttributeError:
        return 'not found'


@dataclasses.dataclass(frozen=True)
class Hardware:
    """Hardware information."""

    architecture: str = field(metadata={'description': 'CPU architecture.'})
    physical_cores: int | None = field(
        metadata={'description': 'CPU physical core count.'},
    )
    logical_cores: int | None = field(
        metadata={'description': 'CPU logical core count.'},
    )
    memory_capacity: float = field(
        metadata={'description': 'Memory capacity in GB.'},
    )

    @classmethod
    def collect(cls) -> Self:
        """Collect hardware information."""
        return cls(
            architecture=platform.machine(),
            physical_cores=psutil.cpu_count(logical=False),
            logical_cores=psutil.cpu_count(logical=True),
            memory_capacity=round(psutil.virtual_memory().total / 1e9, 2),
        )


@dataclasses.dataclass(frozen=True)
class Packages:
    """Python package versions."""

    dask: str = field(metadata={'description': 'Dask/Distributed version.'})
    globus_compute: str = field(
        metadata={'description': 'Globus Compute version.'},
    )
    numpy: str = field(metadata={'description': 'Numpy version.'})
    parsl: str = field(metadata={'description': 'Parsl version.'})
    proxystore: str = field(metadata={'description': 'ProxyStore version.'})
    pydantic: str = field(metadata={'description': 'Pydantic version.'})
    ray: str = field(metadata={'description': 'Ray version.'})
    taps: str = field(metadata={'description': 'TaPS version.'})

    @classmethod
    def collect(cls) -> Self:
        """Collect package version information."""
        return cls(
            dask=_get_version('dask'),
            globus_compute=_get_version('globus_compute_sdk'),
            numpy=_get_version('numpy'),
            parsl=_get_version('parsl'),
            proxystore=_get_version('proxystore'),
            pydantic=_get_version('pydantic'),
            ray=_get_version('ray'),
            taps=_get_version('taps'),
        )


@dataclasses.dataclass(frozen=True)
class Python:
    """Python interpreter information."""

    version: str = field(metadata={'description': 'Python version.'})
    implementation: str = field(
        metadata={'description': 'Python implementation.'},
    )
    compiler: str = field(
        metadata={'description': 'Compiler used to compile this interpreter'},
    )
    bit_length: int = field(
        metadata={
            'description': (
                'Bit-length of the interpreter (e.g., 32 vs 64-bit).'
            ),
        },
    )

    @classmethod
    def collect(cls) -> Self:
        """Collect Python interpreter information."""
        return cls(
            version=platform.python_version(),
            implementation=platform.python_implementation(),
            compiler=platform.python_compiler(),
            bit_length=sys.maxsize.bit_length() + 1,
        )


@dataclasses.dataclass(frozen=True)
class System:
    """System information."""

    hostname: str = field(metadata={'description': 'Network name of node.'})
    platform: str = field(metadata={'description': 'Platform identifier.'})
    platform_ext: str = field(
        metadata={'description': 'Extended platform information.'},
    )

    @classmethod
    def collect(cls) -> Self:
        """Collect system information."""
        return cls(
            hostname=platform.node(),
            platform=sys.platform,
            platform_ext=platform.platform(),
        )


@dataclasses.dataclass(frozen=True)
class Environment:
    """Environment information.

    To view the current environment:
    ```bash
    $ python -m taps.run.env
    system:
      hostname: ...
      os: linux (...)
      cpu: x86_64 (8 cores / 16 logical)
      memory: 16 GB
    python:
      version: 3.11.9
      build: CPython (64-bit runtime) [GCC 11.4.0]
    packages:
      ...
      taps: 0.2.1
    ```
    """

    hardware: Hardware = field(
        metadata={'description': 'Hardware information.'},
    )
    packages: Packages = field(
        metadata={'description': 'Python package versions.'},
    )
    python: Python = field(
        metadata={'description': 'Python interpreter information.'},
    )
    system: System = field(metadata={'description': 'System information.'})

    @classmethod
    def collect(cls) -> Self:
        """Collect information on the current environment."""
        return cls(
            hardware=Hardware.collect(),
            packages=Packages.collect(),
            python=Python.collect(),
            system=System.collect(),
        )

    def format(self) -> str:
        """Format environment as a human-readable string."""
        pcores = self.hardware.physical_cores
        lcores = self.hardware.logical_cores
        impl = self.python.implementation
        compiler = self.python.compiler
        bit_length = self.python.bit_length

        packages = [
            f'{name}: {version}\n'
            for name, version in dataclasses.asdict(self.packages).items()
        ]

        return f"""\
system:
  hostname: {self.system.hostname}
  os: {self.system.platform} ({self.system.platform_ext})
  cpu: {self.hardware.architecture} ({pcores} cores / {lcores} logical)
  memory: {self.hardware.memory_capacity} GB
python:
  version: {self.python.version}
  build: {impl} ({bit_length}-bit runtime) [{compiler}]
packages:
  {'  '.join(packages)}
""".strip()

    def json(self) -> dict[str, Any]:
        """Get environment as JSON-compatible dictionary."""
        return dataclasses.asdict(self)

    def write_json(self, filepath: str | pathlib.Path) -> None:
        """Write environment to JSON file.

        Args:
            filepath: JSON filepath.
        """
        filepath = pathlib.Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(self.json(), f, indent=4, sort_keys=True)


if __name__ == '__main__':
    print(Environment.collect().format())
