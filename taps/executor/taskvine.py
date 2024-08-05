from __future__ import annotations

from ndcctools.taskvine import FuturesExecutor
from pydantic import Field

from taps.executor.config import ExecutorConfig
from taps.executor.config import register

# def _wrap(task, *args, **kwargs):
#     return task(*args, **kwargs)


# class _FuturesExecutor(FuturesExecutor):
#     def __init__(self, *args, **kwargs) -> None:
#         super().__init__(*args, **kwargs)
#         self.lib_installed = set() # defaultdict(lambda: False)

#     def submit(self, fn, *args, **kwargs):
#         if not isinstance(fn, (FutureFunctionCall, FuturePythonTask)):
#             lib_name = f'taskvine-lib'
#             if lib_name not in self.lib_installed:
#                 lib = self.manager.create_library_from_functions(
#                     lib_name,
#                     _wrap,
#                     poncho_env='dummy-env',
#                     add_env=False,
#                     import_modules=[taps.apps.synthetic],
#                     init_command=None,
#                 )
#                 self.manager.install_library(lib)
#                 self.lib_installed.add(lib_name)

#             fn = self.future_funcall(lib_name, '_wrap', (fn, *args), **kwargs)
#             args = ()
#             kwargs = {}

#         return super().submit(fn, *args, **kwargs)


@register(name='taskvine')
class TaskVineConfig(ExecutorConfig):
    """TaskVine configuration.

    Attributes:
        taskvine_port: TaskVine manager port.
    """

    taskvine_workers: int = Field()
    taskvine_port: int | list[int] = Field(
        [9123, 9129],
        description='taskvine manager port(s)',
    )

    def get_executor(self) -> FuturesExecutor:
        """Create an executor instance from the config."""
        return FuturesExecutor(
            manager_name='taskvine-manager',
            opts={
                'min_workers': self.taskvine_workers,
                'max_workers': self.taskvine_workers,
            },
        )
