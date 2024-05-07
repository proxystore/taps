from __future__ import annotations

import argparse
from typing import Sequence

from pydantic import BaseModel


class Config(BaseModel):
    """Base configuration model type."""

    @classmethod
    def add_argument_group(
        cls,
        parser: argparse.ArgumentParser,
        *,
        argv: Sequence[str] | None = None,
        required: bool = True,
    ) -> None:
        """Add model fields as arguments of an argument group on the parser.

        Args:
            parser: Parser to add a new argument group to.
            argv: Optional sequence of string arguments.
            required: Mark arguments without defaults as required.
        """
        group = parser.add_argument_group(cls.__name__)
        for field_name, field_info in cls.model_fields.items():
            arg_name = field_name.replace('_', '-').lower()
            group.add_argument(
                f'--{arg_name}',
                dest=field_name,
                # type=field_info.annotation,
                default=field_info.get_default(),
                required=field_info.is_required() and required,
                help=field_info.description,
            )
