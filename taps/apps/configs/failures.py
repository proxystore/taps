from __future__ import annotations

import sys
from typing import Any
from typing import Dict
from typing import Literal

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from taps.apps import App
from taps.apps import AppConfig
from taps.apps.failures.types import FailureType
from taps.plugins import register


@register('app')
class FailureInjectionConfig(AppConfig, use_enum_values=True):
    """Failure injection configuration."""

    name: Literal['failures'] = 'failures'
    base: str = Field(description='base app to inject failures into')
    config: Dict[str, Any] = Field(  # noqa: UP006
        default_factory=dict,
        description='base app configuration',
    )
    failure_rate: float = Field(1, description='task failure rate')
    failure_type: FailureType = Field(
        FailureType.DEPENDENCY,
        description='task failure type',
    )

    @field_validator('base', mode='before')
    @classmethod
    def _validate_base_app_name(cls, name: str) -> str:
        from taps.plugins import get_app_configs

        apps = set(get_app_configs().keys())
        # Remove the failure app from the allowed base apps
        apps.remove(cls.model_fields['name'].default)

        if name not in apps:
            raise ValueError(
                f'Base app named "{name}" is unknown. '
                f'Supported apps: {", ".join(sorted(apps))}.',
            )

        return name

    @field_validator('failure_rate', mode='after')
    @classmethod
    def _validate_rate(cls, rate: float) -> float:
        if rate < 0 or rate > 1:
            raise ValueError(
                f'Failure rate must be in the range [0, 1]. Got {rate}.',
            )
        return rate

    @model_validator(mode='after')
    def _validate_model(self) -> Self:
        self._get_app_config()
        return self

    def _get_app_config(self) -> AppConfig:
        from taps.plugins import get_app_configs

        config_type = get_app_configs()[self.base]
        return config_type(**self.config)

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.failures.app import FailureInjectionApp

        return FailureInjectionApp(
            base_config=self._get_app_config(),
            failure_rate=self.failure_rate,
            # Because use_enum_values=True, self.failure_type is actually
            # a string and we need to convert it back.
            failure_type=FailureType(self.failure_type),
        )
