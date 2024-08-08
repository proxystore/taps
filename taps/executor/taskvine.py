from __future__ import annotations

import inspect
import sys

from ndcctools.taskvine import FuturesExecutor
from pydantic import Field

import taps.apps
import taps.data
import taps.engine
import taps.executor
import taps.logging
from taps.engine.engine import _TaskWrapper
from taps.executor.config import ExecutorConfig
from taps.executor.config import register


def _wrap(task, *args, **kwargs):
    return task(*args, **kwargs)


class _FuturesExecutor(FuturesExecutor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.lib_installed = set()

    def submit(self, fn, *args, **kwargs):
        if isinstance(fn, _TaskWrapper):
            func_mod = inspect.getmodule(fn.function)
            lib_name = f'{func_mod.__name__}-taskvine-lib'
            if lib_name not in self.lib_installed:
                modules = [
                    # This module
                    sys.modules[__name__],
                    # Common taps modules
                    taps.engine,
                    taps.apps,
                    taps.logging,
                    taps.executor,
                    taps.data,
                    # The module containing the function
                    func_mod,
                ]
                lib = self.manager.create_library_from_functions(
                    lib_name,
                    # fn is a class instance (_TaskWrapper) but TaskVine
                    # needs a top-level function, so we pass a dummy function
                    # that will just execute the _TaskWrapper.
                    _wrap,
                    add_env=False,
                    hoisting_modules=modules,
                )
                self.manager.install_library(lib)
                self.lib_installed.add(lib_name)

            fn = self.future_funcall(lib_name, '_wrap', *(fn, *args), **kwargs)
            args = ()
            kwargs = {}
        else:
            raise NotImplementedError()

        return super().submit(fn, *args, **kwargs)


@register(name='taskvine')
class TaskVineConfig(ExecutorConfig):
    """TaskVine configuration.

    Attributes:
        taskvine_port: TaskVine manager port.
    """

    taskvine_workers: int = Field()
    taskvine_cores: int | None = Field(None)
    taskvine_port: int | list[int] = Field(
        [9123, 9129],
        description='taskvine manager port(s)',
    )

    def get_executor(self) -> FuturesExecutor:
        """Create an executor instance from the config."""
        opts = {
            'min_workers': self.taskvine_workers,
            'max_workers': self.taskvine_workers,
        }
        if self.taskvine_cores is not None:
            opts['cores'] = self.taskvine_cores

        return _FuturesExecutor(manager_name='taskvine-manager', opts=opts)
