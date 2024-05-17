from __future__ import annotations

import webs.wf.cholesky
import webs.wf.choleskytiled

# These imports ensure the register() decorators of each Workflow
# implementation get run. Workflow implementations defined in
# submodules may also require similar imports in the __init__.py of
# the submodule.
import webs.wf.synthetic
