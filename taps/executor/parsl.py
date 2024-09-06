from __future__ import annotations

import multiprocessing
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

import parsl
from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.concurrent import ParslPoolExecutor
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers.base import Launcher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import LocalProvider
from parsl.providers.base import ExecutionProvider
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator

if TYPE_CHECKING:
    from parsl.executors.high_throughput.manager_selector import (
        ManagerSelector,
    )

from taps.executor import ExecutorConfig as TapsExecutorConfig
from taps.plugins import register


@register('executor')
class ParslLocalConfig(TapsExecutorConfig):
    """Local `ParslPoolExecutor` plugin configuration.

    Simple Parsl configuration that uses the
    [`HighThroughputExecutor`][parsl.executors.HighThroughputExecutor]
    on the local node.
    """

    name: Literal['parsl-local'] = Field(
        'parsl-local',
        description='Executor name.',
    )
    workers: Optional[int] = Field(  # noqa: UP007
        None,
        description='Maximum number of parsl workers.',
    )
    run_dir: str = Field(
        'parsl-runinfo',
        description='Parsl run directory within the app run directory.',
    )

    def get_executor(self) -> ParslPoolExecutor:
        """Create an executor instance from the config."""
        workers = (
            self.workers
            if self.workers is not None
            else multiprocessing.cpu_count()
        )
        executor = HighThroughputExecutor(
            label='taps-htex-local',
            max_workers_per_node=workers,
            address=address_by_hostname(),
            cores_per_worker=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
            ),
        )
        config = Config(
            executors=[executor],
            run_dir=self.run_dir,
            initialize_logging=False,
        )
        return ParslPoolExecutor(config)


@register('executor')
class ParslHTExConfig(TapsExecutorConfig):
    """HTEx `ParslPoolExecutor` plugin configuration.

    These parameters are a subset of
    [`parsl.config.Config`][parsl.config.Config].

    For simple, single-node HTEx deployments, prefer
    [`ParslLocalConfig`][taps.executor.parsl.ParslLocalConfig].
    """

    model_config = ConfigDict(extra='allow')  # type: ignore[misc]

    name: Literal['parsl-htex'] = Field(
        'parsl-htex',
        description='Executor name.',
    )
    htex: HTExConfig = Field(description='HTEx configuration.')
    app_cache: Optional[bool] = Field(  # noqa: UP007
        None,
        description='Enable app caching.',
    )
    retries: int = Field(
        0,
        description='Number of task retries in case of task failure.',
    )
    strategy: Optional[str] = Field(  # noqa: UP007
        None,
        description='Block scaling strategy.',
    )
    max_idletime: Optional[float] = Field(  # noqa: UP007
        None,
        description='Idle time before strategy can shutdown unused blocks.',
    )
    monitoring: Optional[MonitoringConfig] = Field(  # noqa: UP007
        None,
        description='Database monitoring configuration.',
    )
    run_dir: str = Field(
        'parsl-runinfo',
        description='Parsl run directory within the app run directory.',
    )

    def get_executor(self) -> ParslPoolExecutor:
        """Create an executor instance from the config."""
        options = self.model_dump(
            exclude={'name', 'htex', 'monitoring'},
            exclude_none=True,
        )

        if self.monitoring is not None:
            options['monitoring'] = self.monitoring.get_monitoring()

        config = Config(
            executors=[self.htex.get_executor()],
            initialize_logging=False,
            **options,
        )
        return ParslPoolExecutor(config)


class HTExConfig(BaseModel):
    """Configuration for Parl's [`parsl.executors.HighThroughputExecutor`][parsl.executors.HighThroughputExecutor].

    Note:
        Optional attributes will default to Parsl's default values.

    Note:
        Extra options passed to this model will be provided as keyword
        arguments to
        [`parsl.executors.HighThroughputExecutor`][parsl.executors.HighThroughputExecutor].
    """  # noqa: E501

    model_config = ConfigDict(extra='allow')

    provider: Optional[ProviderConfig] = Field(  # noqa: UP007
        None,
        description='Configuration for the compute resource provider.',
    )
    address: Optional[Union[str, AddressConfig]] = Field(  # noqa: UP007
        None,
        description='Address to connect to the main Parsl process.',
    )
    manager_selector: Optional[ManagerSelectorConfig] = Field(  # noqa: UP007
        None,
        description=(
            'Configuration for the manager selector (available in '
            'Parsl v2024.8.5 and later).'
        ),
    )
    worker_ports: Optional[Tuple[int, int]] = Field(  # noqa: UP006,UP007
        None,
        description='Ports used by workers to connect to Parsl',
    )
    worker_port_range: Optional[Tuple[int, int]] = Field(  # noqa: UP006,UP007
        None,
        description='Range of ports to choose worker ports from.',
    )
    interchange_port_range: Optional[Tuple[int, int]] = Field(  # noqa: UP006,UP007
        None,
        description='Ports used by Parsl to connect to interchange.',
    )

    def get_executor(self) -> HighThroughputExecutor:
        """Create an executor instance from the config."""
        options = self.model_dump(exclude_none=True)
        if self.model_extra is not None:  # pragma: no branch
            options.update(self.model_extra)

        if self.provider is not None:
            options['provider'] = self.provider.get_provider()
        if self.manager_selector is not None:
            options['manager_selector'] = (
                self.manager_selector.get_manager_selector()
            )
        if self.address is not None and isinstance(
            self.address,
            AddressConfig,
        ):
            options['address'] = self.address.get_address()

        return HighThroughputExecutor(label='taps-htex', **options)


class AddressConfig(BaseModel):
    """Parsl address configuration.

    Example:
        ```python
        from parsl.addresses import address_by_interface
        from taps.executor.parsl import AddressConfig

        config = AddressConfig(kind='address_by_interface', ifname='bond0')
        assert config.get_address() == address_by_interface(ifname='bond0')
        ```
    """

    model_config = ConfigDict(extra='allow')

    kind: str = Field(description='Function to invoke to get address.')

    @field_validator('kind')
    @classmethod
    def _validate_address_name(cls, kind: str) -> str:
        # Parse the class name if the full path is passed. For example,
        # parsl.addresses.address_by_hostname and address_by_hostname should
        # both be valid.
        cls_name = kind.split('.')[-1]
        try:
            getattr(parsl.addresses, cls_name)
        except AttributeError as e:
            raise ValueError(
                'The module parsl.addresses does not contain a provider '
                f'named {cls_name}.',
            ) from e

        return cls_name

    def get_address(self) -> str:
        """Get the address according to the configuration."""
        address_fn = getattr(parsl.addresses, self.kind)
        options = self.model_extra if self.model_extra is not None else {}
        return address_fn(**options)


class ProviderConfig(BaseModel):
    """Parsl execution provider configuration.

    Example:
        Create a provider configuration and call
        [`get_provider()`][taps.executor.parsl.ProviderConfig.get_provider].
        ```python
        from taps.executor.parsl import ProviderConfig

        config = ProviderConfig(
            kind='PBSProProvider',
            account='my-account',
            cpus_per_node=32,
            init_blocks=1,
            max_blocks=1,
            min_blocks=0,
            nodes_per_block=1,
            queue='debug',
            select_options='ngpus=4',
            walltime='00:30:00',
            worker_init='module load conda',
        )
        config.get_provider()
        ```
        The resulting provider is equivalent to creating it manually.
        ```python
        from parsl.providers import PBSProProvider

        PBSProProvider(
            account='my-account',
            cpus_per_node=32,
            init_blocks=1,
            max_blocks=1,
            min_blocks=0,
            nodes_per_block=1,
            queue='debug',
            select_options='ngpus=4',
            walltime='00:30:00',
            worker_init='module load conda',
        ),
        ```
    """

    model_config = ConfigDict(extra='allow')

    kind: str = Field(description='Execution provider class name')
    launcher: Optional[LauncherConfig] = Field(  # noqa: UP007
        None,
        description='Launcher configuration.',
    )

    @field_validator('kind')
    @classmethod
    def _validate_provider_name(cls, kind: str) -> str:
        # Parse the class name if the full path is passed. For example,
        # parsl.providers.SlurmProvider and SlurmProvider should both be valid.
        cls_name = kind.split('.')[-1]
        try:
            getattr(parsl.providers, cls_name)
        except AttributeError as e:
            raise ValueError(
                'The module parsl.providers does not contain a provider '
                f'named {cls_name}.',
            ) from e

        return cls_name

    def get_provider(self) -> ExecutionProvider:
        """Create a provider from the configuration."""
        options = self.model_extra if self.model_extra is not None else {}

        if self.launcher is not None:
            options['launcher'] = self.launcher.get_launcher()

        provider_cls = getattr(parsl.providers, self.kind)
        return provider_cls(**options)


class LauncherConfig(BaseModel):
    """Parsl launcher configuration.

    Example:
        Create a launcher configuration and call
        [`get_launcher()`][taps.executor.parsl.LauncherConfig.get_launcher].
        ```python
        from taps.executor.parsl import LauncherConfig

        config = LauncherConfig(
            kind='MpiExecLauncher',
            bind_cmd='--cpu-bind',
            overrides='--depth=64 --ppn=1,
        )
        config.get_launcher()
        ```
        The resulting launcher is equivalent to creating it manually.
        ```python
        from parsl.launchers import MpiExecLauncher

        MpiExecLauncher(bind_cmd='--cpu-bind', overrides='--depth=64 --ppn 1')
        ```
    """

    model_config = ConfigDict(extra='allow')

    kind: str = Field(description='Launcher class name.')

    @field_validator('kind')
    @classmethod
    def _validate_launcher_name(cls, kind: str) -> str:
        # Parse the class name if the full path is passed. For example,
        # parsl.launchers.SrunLauncher and SrunLauncher should both be valid.
        cls_name = kind.split('.')[-1]
        try:
            getattr(parsl.launchers, cls_name)
        except AttributeError as e:
            raise ValueError(
                'The module parsl.launchers does not contain a provider '
                f'named {cls_name}.',
            ) from e

        return cls_name

    def get_launcher(self) -> Launcher:
        """Create a launcher from the configuration."""
        launcher_cls = getattr(parsl.launchers, self.kind)
        options = self.model_extra if self.model_extra is not None else {}
        return launcher_cls(**options)


class ManagerSelectorConfig(BaseModel):
    """Parsl HTEx manager selector configuration.

    Example:
        Create a manager selector configuration and call
        [`get_manager_selector()`][taps.executor.parsl.ManagerSelectorConfig.get_manager_selector].
        ```python
        from taps.executor.parsl import ManagerSelectorConfig

        config = ManagerSelectorConfig(kind='RandomManagerSelector')
        config.get_manager_selector()
        ```
        The resulting manager selector is equivalent to creating it manually.
        ```python
        from parsl.executors.high_throughput.manager_selector import RandomManagerSelector

        RandomManagerSelector()
        ```
    """  # noqa: E501

    model_config = ConfigDict(extra='allow')

    kind: str = Field(description='Manager selector class name.')

    @field_validator('kind')
    @classmethod
    def _validate_manager_selector_name(cls, kind: str) -> str:
        # Parse the class name if the full path is passed. For example,
        # parsl.executors.high_throughput.manager_selector.RandomManagerSelector  # noqa: E501
        # and RandomManagerSelector should both be valid.
        cls_name = kind.split('.')[-1]
        try:
            import parsl.executors.high_throughput.manager_selector
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                'Cannot import manager_selector module from Parsl. '
                'Manager selector configuration requires Parsl version '
                'v2024.8.5 or later.',
            ) from e

        try:
            getattr(parsl.executors.high_throughput.manager_selector, cls_name)
        except AttributeError as e:
            raise ValueError(
                'The module parsl.executors.high_throughput.manager_selector '
                f'does not contain a type named {cls_name}.',
            ) from e

        return cls_name

    def get_manager_selector(self) -> ManagerSelector:
        """Create a manager selector from the configuration."""
        manager_cls = getattr(
            parsl.executors.high_throughput.manager_selector,
            self.kind,
        )
        options = self.model_extra if self.model_extra is not None else {}
        return manager_cls(**options)


class MonitoringConfig(BaseModel):
    """Parsl monitoring system configuration.

    Example:
        Create a monitoring configuration and call
        [`get_monitoring()`][taps.executor.parsl.MonitoringConfig.get_monitoring].
        ```python
        from taps.executor.parsl import MonitoringConfig

        config = MonitoringConfig(
            hub_address='localhost',
            logging_endpoint='sqlite:///parsl-runinfo/monitoring.db',
            resource_monitoring_interval=1,
            hub_port=55055,
        )
        config.get_monitoring()
        ```
        The resulting `MonitoringHub` is equivalent to creating it manually.
        ```python
        from parsl.monitoring.monitoring import MonitoringHub

        MonitoringHub(
            hub_address='localhost',
            logging_endpoint='sqlite:///parsl-runinfo/monitoring.db',
            resource_monitoring_interval=1,
            hub_port=55055,
        )
        ```
    """

    model_config = ConfigDict(extra='allow')

    hub_address: Optional[Union[str, AddressConfig]] = Field(  # noqa: UP007
        None,
        description='Address to connect to the monitoring hub.',
    )
    hub_port_range: Optional[Tuple[int, int]] = Field(  # noqa: UP006,UP007
        None,
        description='Port range for a ZMQ channel from executor process.',
    )

    def get_monitoring(self) -> MonitoringHub:
        """Create a MonitoringHub from the configuration."""
        options = self.model_dump(exclude_none=True)
        if self.model_extra is not None:  # pragma: no branch
            options.update(self.model_extra)
        if self.hub_address is not None and isinstance(
            self.hub_address,
            AddressConfig,
        ):
            options['hub_address'] = self.hub_address.get_address()

        return MonitoringHub(**options)
