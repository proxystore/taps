from __future__ import annotations

# These imports ensure the register() decorators of each Workflow
# implementation get run. Workflow implementations defined in
# submodules may also require similar imports in the __init__.py of
# the submodule.
import webs.wf.cholesky
import webs.wf.synthetic
