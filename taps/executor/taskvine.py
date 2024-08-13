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
    def __init__(self, *args, serverless: bool, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.lib_installed = set()
        self.user_cores = kwargs['opts']['cores']
        self.serverless = serverless

    def submit(self, fn, *args, **kwargs):
        if isinstance(fn, _TaskWrapper) and self.serverless:
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
                lib.set_cores(self.user_cores)
                lib.set_function_slots(self.user_cores)
                self.manager.install_library(lib)
                self.lib_installed.add(lib_name)

            fn = self.future_funcall(lib_name, '_wrap', *(fn, *args), **kwargs)
            fn.set_cores(1)
            args = ()
            kwargs = {}
        else:
            fn = self.future_task(fn, *args, **kwargs)
            fn.set_cores(1)
            self.task_table.append(fn)

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
    taskvine_manager: bool = Field(False)
    taskvine_serverless: bool = Field(True)

    def get_executor(self) -> FuturesExecutor:
        """Create an executor instance from the config."""
        opts = {
            'min_workers': self.taskvine_workers,
            'max_workers': self.taskvine_workers,
        }
        if self.taskvine_cores is not None:
            opts['cores'] = self.taskvine_cores

        ex = _FuturesExecutor(
            manager_name='taskvine-manager',
            opts=opts,
            factory=not self.taskvine_manager,
            serverless=self.taskvine_serverless,
        )
        # ex.manager.tune('watch-library-logfiles', 1)
        return ex
