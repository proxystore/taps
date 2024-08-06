from __future__ import annotations

import logging
import platform
import sys
from typing import NamedTuple

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import psutil

import taps

logger = logging.getLogger(__name__)


class Environment(NamedTuple):
    """Named tuple representing collected environment information.

    To view the current environment:
    ```bash
    $ python -m taps.run.env
    Host: ...
    OS: linux (...)
    CPU: x86_64 (8 cores / 16 logical)
    RAM: 15.66 GB
    Python version: 3.11.5
    Python build: CPython (64-bit runtime) [GCC 11.4.0]
    TaPS version: 0.2.0
    ```
    """

    platform: str
    platform_ext: str
    host: str
    cpu_architecture: str
    cpu_physical_cores: int
    cpu_logical_cores: int
    memory_gb: float
    python_version: str
    python_implementation: str
    python_compiler: str
    python_bit_length: int
    taps_version: str

    @classmethod
    def collect(cls) -> Self:
        """Collect information on the current environment."""
        return cls(
            platform=sys.platform,
            platform_ext=platform.platform(),
            host=platform.node(),
            cpu_architecture=platform.machine(),
            cpu_physical_cores=psutil.cpu_count(logical=False),
            cpu_logical_cores=psutil.cpu_count(logical=True),
            memory_gb=round(psutil.virtual_memory().total / 1e9, 2),
            python_version=platform.python_version(),
            python_implementation=platform.python_implementation(),
            python_compiler=platform.python_compiler(),
            python_bit_length=sys.maxsize.bit_length() + 1,
            taps_version=taps.__version__,
        )

    def format(self) -> str:
        """Format environment as a human-readable string."""
        pcores, lcores = self.cpu_physical_cores, self.cpu_logical_cores
        impl = self.python_implementation
        compiler = self.python_compiler
        bit_length = self.python_bit_length

        return f"""\
host: {self.host}
  os: {self.platform} ({self.platform_ext})
  cpu: {self.cpu_architecture} ({pcores} cores / {lcores} logical)
  memory: {self.memory_gb} GB
python:
  version: {self.python_version}
  build: {impl} ({bit_length}-bit runtime) [{compiler}]
  taps: {self.taps_version}
""".strip()


if __name__ == '__main__':
    print(Environment.collect().format())
