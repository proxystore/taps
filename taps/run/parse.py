from __future__ import annotations

import argparse
import functools
import sys
from typing import Any
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    import tomllib
else:  # pragma: <3.11 cover
    import tomli as tomllib

from pydantic_settings import CliSettingsSource

from taps import plugins
from taps.run.config import _make_config_cls
from taps.run.config import Config
from taps.run.utils import flatten_mapping


def _parse_toml_options(filepath: str | None) -> dict[str, Any]:
    if filepath is None:
        return {}

    with open(filepath, 'rb') as f:
        return tomllib.load(f)


class _ArgparseFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


def _add_argument(
    parser: argparse.ArgumentParser,
    *names: str,
    **kwargs: Any,
) -> None:
    if any(name.endswith('.name') for name in names):
        # These options are the 'name' attribute of a plugin. The
        # CLI options for these are added manually in parse_args_to_config
        # so do not need to be added again by CliSettingsSource.
        return

    parser.add_argument(*names, **kwargs)


def _add_argument_group(
    parser: argparse.ArgumentParser,
    **kwargs: Any,
) -> argparse._ArgumentGroup:
    title = kwargs.get('title', None)

    # Note: this accesses a private part of the ArgumentParser
    # API so could be an issue in the future. But to my knowledge,
    # this is the only way to prevent the CliSettingsSource from
    # making duplicate parser groups from what we manually create.
    for group in parser._action_groups:
        if group.title == title:
            return group

    return parser.add_argument_group(**kwargs)


def parse_args_to_config(argv: Sequence[str]) -> Config:
    """Construct an argument parser and parse string arguments to a config.

    Args:
        argv: Sequence of string arguments.

    Returns:
        Configuration.
    """
    apps = plugins.get_app_configs()

    parser = argparse.ArgumentParser(
        description="""\
Task Performance Suite (TaPS) CLI.

Application benchmarks can be configured via CLI options, a TOML
configuration file, or a mix of both. CLI options take precedence
over configuration files.

The default behavior of -h/--help is to show only the minimally
relevant set of options. For example, only the process-pool
executor options will be shown if --engine.executor process-pool
is specified; the options for other executors will be suppressed.
This behavior applies to all plugin types.
""",
        prog='python -m taps.run',
        formatter_class=_ArgparseFormatter,
    )
    parser.add_argument(
        '--config',
        '-c',
        default=argparse.SUPPRESS,
        help='base toml configuration file to load',
    )

    app_group = parser.add_argument_group('app options')
    app_group.add_argument(
        '--app',
        choices=list(apps.keys()),
        dest='app.name',
        metavar='APP',
        help='app choice {%(choices)s}',
    )

    engine_group = parser.add_argument_group('engine options')
    engine_group.add_argument(
        '--engine.executor',
        '--executor',
        choices=list(plugins.get_executor_configs().keys()),
        default=argparse.SUPPRESS,
        dest='engine.executor.name',
        metavar='EXECUTOR',
        help='executor choice {%(choices)s} (default: process-pool)',
    )
    engine_group.add_argument(
        '--engine.filter',
        '--filter',
        choices=list(plugins.get_filter_configs().keys()),
        default=argparse.SUPPRESS,
        dest='engine.filter.name',
        metavar='FILTER',
        help='filter choice {%(choices)s} (default: null)',
    )
    engine_group.add_argument(
        '--engine.transformer',
        '--transformer',
        choices=list(plugins.get_transformer_configs().keys()),
        default=argparse.SUPPRESS,
        dest='engine.transformer.name',
        metavar='TRANSFORMER',
        help='transformer choice {%(choices)s} (default: null)',
    )

    if len(argv) == 0 or argv[0] in ['-h', '--help']:
        # Shortcut to print help output if no args or just -h/--help
        # are provided.
        parser.parse_args(['--help'])  # pragma: no cover

    # Strip --help from argv so we can quickly parse the base options
    # to figure out which config types we will need to use. --help
    # will be parsed again by CliSettingsSource.
    _argv = list(filter(lambda v: v not in ['-h', '--help'], argv))
    base_options = vars(parser.parse_known_args(_argv)[0])
    base_options = {k: v for k, v in base_options.items() if v is not None}
    config_file = base_options.pop('config', None)
    toml_options = flatten_mapping(_parse_toml_options(config_file))

    # base_options takes precedence over toml_options if there are
    # matching keys.
    base_options = {**toml_options, **base_options}
    if 'app.name' not in base_options or base_options['app.name'] is None:
        raise ValueError(
            'Missing the app name option. Either provides --app {APP} via '
            'the CLI args or add the app.name attribute the config file.',
        )

    app_group.description = f'selected app: {base_options["app.name"]}'

    settings_cls = _make_config_cls(base_options)
    base_namespace = argparse.Namespace(**base_options)

    cli_settings = CliSettingsSource(
        settings_cls,
        cli_avoid_json=True,
        cli_parse_args=argv,
        cli_parse_none_str='none',
        cli_use_class_docs_for_groups=False,
        root_parser=parser,
        add_argument_method=_add_argument,
        add_argument_group_method=_add_argument_group,
        parse_args_method=functools.partial(
            argparse.ArgumentParser.parse_args,
            namespace=base_namespace,
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    return settings_cls(_cli_settings_source=cli_settings)
