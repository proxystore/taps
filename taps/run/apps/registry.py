from __future__ import annotations

from typing import Callable
from typing import TypeVar

from taps.app import AppConfig

_REGISTERED_APP_CONFIGS: dict[str, type[AppConfig]] = {}
AppConfigT = TypeVar('AppConfigT', bound=AppConfig)
"""Application config type."""


def register_app(
    *,
    name: str,
) -> Callable[[type[AppConfigT]], type[AppConfigT]]:
    """Decorator for registering an app config type.

    Example:
        An app config can be defined and registered using a name.
        ```python
        from pydantic import Field

        from taps.apps.app import App
        from taps.config import Config
        from taps.run.app import register_app

        @register('foo')
        class FooConfig(Config):
            n: int = Field(1, description='count')

            def get_app(self) -> App:
                from taps.apps.foo import FooApp

                return FooApp(n=self.n)
        ```

        Registration will make the app named "foo" available within the CLI.
        ```bash
        python -m taps.run foo --n 1 ...
        ```
        ```

    Args:
        name: Name of the application. This will be used as the app option
            within the CLI.
    """

    def _decorator(cls: type[AppConfigT]) -> type[AppConfigT]:
        _REGISTERED_APP_CONFIGS[name] = cls
        return cls

    return _decorator


def get_registered_apps() -> dict[str, type[AppConfig]]:
    """Get all registered application configs.

    Returns:
        Mapping of application name to the config type.
    """
    return _REGISTERED_APP_CONFIGS.copy()
