from __future__ import annotations

# These imports are needed to ensure the executor
# config registration decorator in each file is run.
import taps.executor.dask
import taps.executor.globus
import taps.executor.parsl
import taps.executor.python
import taps.executor.ray
