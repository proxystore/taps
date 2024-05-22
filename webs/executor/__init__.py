from __future__ import annotations

# These imports are needed to ensure the executor
# config registration decorator in each file is run.
import webs.executor.dask
import webs.executor.globus
import webs.executor.parsl
import webs.executor.python
import webs.executor.ray
