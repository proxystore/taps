from __future__ import annotations

import sys
from typing import Callable
from typing import Literal
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

from pydantic import BaseModel

from taps.apps import AppConfig

if TYPE_CHECKING:
    from taps.executor import ExecutorConfig
    from taps.filter import FilterConfig
    from taps.transformer import DataTransformerConfig

_REGISTERED_APP_CONFIGS: dict[str, type[AppConfig]] = {}
_REGISTERED_EXECUTOR_CONFIGS: dict[str, type[ExecutorConfig]] = {}
_REGISTERED_FILTER_CONFIGS: dict[str, type[FilterConfig]] = {}
_REGISTERED_TRANSFORMER_CONFIGS: dict[str, type[DataTransformerConfig]] = {}

_REGISTERED_CONFIGS = {
    'app': _REGISTERED_APP_CONFIGS,
    'executor': _REGISTERED_EXECUTOR_CONFIGS,
    'filter': _REGISTERED_FILTER_CONFIGS,
    'transformer': _REGISTERED_TRANSFORMER_CONFIGS,
}

ConfigType = TypeVar('ConfigType', bound=BaseModel)
PluginType: TypeAlias = Literal['app', 'executor', 'filter', 'transformer']


def register(
    kind: PluginType,
) -> Callable[[type[ConfigType]], type[ConfigType]]:
    """Decorator for registering plugin configurations.

    Example:
        An app config can be defined and registered using a name.
        ```python
        from concurrent.futures import Executor

        from pydantic import Field

        from taps.plugins import register
        from taps.executor.config import ExecutorConfig

        @register('executor')
        class FooConfig(ExecutorConfig):
            name: Literal['foo'] = 'foo'
            n: int = Field(1, description='count')

            def get_executor(self) -> Executor:
                ...
        ```

        Registration will make the executor named "foo" available within
        the CLI and configuration files.
        ```

    Args:
        kind: Kind of plugin that is being registered.
    """

    def _decorator(cls: type[ConfigType]) -> type[ConfigType]:
        try:
            registry = _REGISTERED_CONFIGS[kind]
        except KeyError:
            raise ValueError(f'Unknown plugin type "{kind}".') from None

        try:
            name = cls.model_fields['name'].default
            registry[name] = cls  # type: ignore[index]
        except Exception as e:
            raise RuntimeError(
                f'Failed to register {cls.__name__} as a {kind} plugin.',
            ) from e

        return cls

    return _decorator


def get_app_configs() -> dict[str, type[AppConfig]]:
    """Get all registered application configs.

    Returns:
        Mapping of application name to the config type.
    """
    return _REGISTERED_APP_CONFIGS.copy()


def get_executor_configs() -> dict[str, type[ExecutorConfig]]:
    """Get all registered executor configs.

    Returns:
        Mapping of executor name to the config type.
    """
    return _REGISTERED_EXECUTOR_CONFIGS.copy()


def get_filter_configs() -> dict[str, type[FilterConfig]]:
    """Get all registered filter configs.

    Returns:
        Mapping of filter name to the config type.
    """
    return _REGISTERED_FILTER_CONFIGS.copy()


def get_transformer_configs() -> dict[str, type[DataTransformerConfig]]:
    """Get all registered transformer configs.

    Returns:
        Mapping of transformer name to the config type.
    """
    return _REGISTERED_TRANSFORMER_CONFIGS.copy()


# Ensure that register() decorators on app configs get executed.
# This is at the end of the file because register() needs to be initialized
# before we import the app configs because those module import and use
# register(). Otherwise, we'd get an AttributeError on register() due to
# a circular import.
import taps.apps.configs  # noqa: E402, F401
