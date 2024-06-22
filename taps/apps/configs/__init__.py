from __future__ import annotations

import pkgutil

# Import all of the submodules on this module. This is done
# to ensure that the register decorators on each app config
# declaration get executed.
__path__ = pkgutil.extend_path(__path__, __name__)
for module in pkgutil.walk_packages(path=__path__, prefix=f'{__name__}.'):
    __import__(module.name)
