from __future__ import annotations

import contextlib
import logging
import os
import pathlib
from collections.abc import Mapping
from collections.abc import MutableMapping
from typing import Any
from typing import Generator

from pydantic import BaseModel
from pydantic import ValidationError

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def change_cwd(
    dest: pathlib.Path | str,
) -> Generator[pathlib.Path, None, None]:
    """Context manager that changes the current working directory.

    Args:
        dest: Path to temporarily change the working directory to.

    Yields:
        Destination directory as an absolute [`Path`][pathlib.Path].
    """
    origin = pathlib.Path.cwd().absolute()
    dest = pathlib.Path(dest).absolute()
    dest.mkdir(parents=True, exist_ok=True)
    os.chdir(dest)

    try:
        yield dest
    finally:
        os.chdir(origin)


def flatten_mapping(
    mapping: MutableMapping[str, Any],
    parent_key: str = '',
    separator: str = '.',
) -> dict[str, Any]:
    """Flatten the keys of nested mappings/dicts.

    Args:
        mapping: Nested mapping to flatten.
        parent_key: Prefix of parent keys when this function is called
            recursively.
        separator: Separator between key parts.

    Returns:
        Flattened dictionary.
    """
    items: list[tuple[str, Any]] = []

    for key, value in mapping.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(
                flatten_mapping(value, new_key, separator=separator).items(),
            )
        else:
            items.append((new_key, value))
    return dict(items)


def prettify_mapping(
    mapping: Mapping[str, Any],
    level: int = 0,
    indent: int = 2,
) -> str:
    """Turn a mapping into a nicely formatted string.

    Example:
        ```python
        >>> from taps.run.utils import prettify_mapping
        >>> data = {'a': {'b': [1, 2, 3], 'name': 'foo'}, 'c': 'baz', 'b': 'bar'}
        >>> print(prettify_mapping(data))
        a:
          name: 'foo'
          b: [1, 2, 3]
        b: 'bar'
        c: 'baz'
        ```
    """  # noqa: E501
    lines: list[str] = []
    space = ' ' * indent * level

    keys = sorted(mapping.keys())

    # Move the 'name' key to the front
    if 'name' in keys:
        keys.remove('name')
        keys = ['name', *keys]

    for key in keys:
        value = mapping[key]

        if isinstance(value, Mapping):
            lines.append(f'{space}{key}:')
            if len(value) > 0:
                lines.append(prettify_mapping(value, level + 1, indent))
        else:
            lines.append(f'{space}{key}: {value!r}')

    return '\n'.join(lines)


def prettify_validation_error(
    error: ValidationError,
    model: type[BaseModel] | None = None,
) -> ValueError:
    """Parse a Pydantic validation error into a ValueError.

    Given a [`ValidationError`][pydantic_core.ValidationError],
    ```
    pydantic_core._pydantic_core.ValidationError: 2 validation errors for GeneratedConfig
    app.matrix_size
      Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='100x', input_type=str]
        For further information visit https://errors.pydantic.dev/2.7/v/int_parsing
    engine.executor.max_threads
      Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='1.5', input_type=str]
        For further information visit https://errors.pydantic.dev/2.7/v/int_parsing
    ```
    returns a [`ValueError`][ValueError] with a more readable output.
    ```
    ValueError: Found 2 validation errors:
      - Input should be a valid integer, unable to parse string as an integer
        Attribute: app.matrix_size
        Input (str): '100x'
        Error type: int_parsing (https://errors.pydantic.dev/2.7/v/int_parsing)
      - Input should be a valid integer, unable to parse string as an integer
        Attribute: engine.executor.max_threads
        Input (str): '1.5'
        Error type: int_parsing (https://errors.pydantic.dev/2.7/v/int_parsing)
    ```
    """  # noqa: E501
    errors: list[str] = []

    for e in error.errors():
        attribute = '.'.join(str(v) for v in e['loc'])
        input_ = e['input']
        message = e['msg']
        type_ = e['type']
        url = e.get(
            'url',
            'https://docs.pydantic.dev/latest/errors/validation_errors/',
        )

        errors.append(f"""\
  - {message}
    Attribute: {attribute}
    Input ({type(input_).__name__}): {input_!r}
    Error type: {type_} ({url})\
""")

    errors_str = '\n'.join(errors)
    count = error.error_count()

    if model is not None:
        model_str = f' for {model.__module__}.{model.__name__}'
    else:
        model_str = ''

    return ValueError(f"""\
Found {count} validation error{"" if count == 1 else "s"}{model_str}
{errors_str}\
""")


@contextlib.contextmanager
def update_environment(
    variables: Mapping[str, str],
) -> Generator[None, None, None]:
    """Context manager that updates environment variables.

    Args:
        variables: Mapping of environment variable name to value to
            temporarily set.
    """
    previous = {
        name: value for name, value in os.environ.items() if name in variables
    }
    os.environ.update(variables)
    logger.debug(
        f'Updated {len(variables)} environment variable(s) '
        f'({", ".join(variables.keys())})',
    )
    try:
        yield
    finally:
        # Remove the added variables then restore any old variables
        for name in variables:
            os.environ.pop(name)
        os.environ.update(previous)
        if len(previous) > 0:
            logger.debug(
                f'Restored {len(previous)} environment variable(s) '
                f'({",".join(previous.keys())})',
            )
