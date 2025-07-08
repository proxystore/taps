from __future__ import annotations

import dataclasses
import json
import pathlib
import sys
from typing import Any
from typing import cast
from typing import Dict
from typing import Literal
from typing import TypeVar
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import extract
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from proxystore.store.config import ConnectorConfig
from proxystore.store.utils import resolve_async
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from taps.plugins import register
from taps.transformer._protocol import TransformerConfig

T = TypeVar('T')
JSON = Union[int, float, str, Dict[str, 'JSON']]  # noqa: UP006
_PROXYSTORE_DIR = 'proxystore'
_PROXYSTORE_AGGREGATE_FILE = 'aggregated.json'
_PROXYSTORE_STATS_FILE = 'stats.jsonl'


@register('transformer')
class ProxyTransformerConfig(TransformerConfig):
    """[`ProxyTransformer`][taps.transformer.ProxyTransformer] plugin configuration.

    Note:
        Extra arguments provided to this config will be passed as parameters
        to the [`Store`][proxystore.store.Store].
    """  # noqa: E501

    model_config = ConfigDict(extra='allow')  # type: ignore[misc]

    name: Literal['proxystore'] = Field(
        'proxystore',
        description='Transformer name.',
    )
    connector: ConnectorConfig = Field(
        description='Connector configuration.',
    )
    async_resolve: bool = Field(
        False,
        description=(
            'Asynchronously resolve proxies. Not compatible with '
            'extract_target=True.'
        ),
    )
    cache_size: int = Field(16, description='cache size')
    extract_target: bool = Field(
        False,
        description=(
            'Extract the target from the proxy when resolving the identifier. '
            'Not compatible with async_resolve=True.'
        ),
    )
    metrics: bool = Field(
        False,
        description='Enable recording operation metrics.',
    )
    populate_target: bool = Field(
        True,
        description='Populate target objects of newly created proxies.',
    )

    @model_validator(mode='after')
    def _validate_mutex_options(self) -> Self:
        if self.async_resolve and self.extract_target:
            raise ValueError(
                'Options async_resolve and extract_target cannot be '
                'enabled at the same time.',
            )
        return self

    def get_transformer(self) -> ProxyTransformer:
        """Create a transformer from the configuration."""
        connector = self.connector.get_connector()

        # Want register=True to be the default unless the user config
        # has explicitly disabled it.
        extra: dict[str, Any] = {'register': True}
        # Guaranteed when config.extra is set to "allow"
        assert self.model_extra is not None
        extra.update(self.model_extra)

        return ProxyTransformer(
            store=Store(
                'proxy-transformer',
                connector=connector,
                cache_size=self.cache_size,
                metrics=self.metrics,
                populate_target=self.populate_target,
                **extra,
            ),
            async_resolve=self.async_resolve,
            extract_target=self.extract_target,
            metrics_dir=_PROXYSTORE_DIR if self.metrics else None,
        )


class ProxyTransformer:
    """Proxy object transformer.

    Transforms objects into proxies which act as the identifier.

    Args:
        store: Store instance to use for proxying objects.
        async_resolve: Begin asynchronously resolving proxies when the
            transformer resolves a proxy (which is otherwise a no-op unless
            `extract_target=True`). Not compatible with `extract_target=True`.
        extract_target: When `True`, resolving an identifier (i.e., a proxy)
            will return the target object. Otherwise, the proxy is returned
            since a proxy can act as the target object. Not compatible
            with `async_resolve=True`.
        metrics_dir: If metrics recording on `store` is `True`, then
            write the recorded metrics to this directory when this transformer
            is closed. Typically, `close()` is only called on the transformer
            instance in the main TaPS process (i.e., `close()` is not called
            in worker processes) so only the metrics from the main process
            will be recorded.
    """

    def __init__(
        self,
        store: Store[Any],
        *,
        async_resolve: bool = False,
        extract_target: bool = False,
        metrics_dir: str | None = None,
    ) -> None:
        if async_resolve and extract_target:
            raise ValueError(
                'Options async_resolve and extract_target cannot be '
                'enabled at the same time.',
            )

        self.store = store
        self.async_resolve = async_resolve
        self.extract_target = extract_target
        self.metrics_dir = (
            pathlib.Path(metrics_dir).resolve()
            if metrics_dir is not None
            else None
        )

    def __repr__(self) -> str:
        ctype = type(self).__name__
        store = f'store={self.store}'
        async_ = f'async_resolve={self.async_resolve}'
        extract = f'extract_target={self.extract_target}'
        metrics = f'metrics_dir={self.metrics_dir}'
        return f'{ctype}({store}, {async_}, {extract}, {metrics})'

    def __getstate__(self) -> dict[str, Any]:
        return {
            'config': self.store.config(),
            'async_resolve': self.async_resolve,
            'extract_target': self.extract_target,
            'metrics_dir': self.metrics_dir,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        store = get_store(state['config'].name)
        if store is not None:
            self.store = store
        else:
            self.store = Store.from_config(state['config'])
        self.async_resolve = state['async_resolve']
        self.extract_target = state['extract_target']
        self.metrics_dir = state['metrics_dir']

    def close(self) -> None:
        """Close the transformer."""
        self.store.close()

        if self.metrics_dir is not None:
            _write_metrics(
                self.store,
                self.metrics_dir / _PROXYSTORE_AGGREGATE_FILE,
                self.metrics_dir / _PROXYSTORE_STATS_FILE,
            )

    def is_identifier(self, obj: Any) -> bool:
        """Check if the object is an identifier instance."""
        return isinstance(obj, Proxy)

    def transform(self, obj: T) -> Proxy[T]:
        """Transform the object into an identifier.

        Args:
            obj: Object to transform.

        Returns:
            Identifier object that can be used to resolve `obj`.
        """
        return self.store.proxy(obj)

    def resolve(self, identifier: Proxy[T]) -> T | Proxy[T]:
        """Resolve an object from an identifier.

        Args:
            identifier: Identifier to an object.

        Returns:
            The resolved object or a proxy of the resolved object depending \
            on the setting of `extract_target`.
        """
        if self.extract_target:
            return extract(identifier)
        if self.async_resolve:
            resolve_async(identifier)
        return identifier


def _format_metrics(
    store: Store[Any],
) -> tuple[dict[str, JSON], list[JSON]] | None:
    if store.metrics is None:
        return None

    aggregated = {
        key: cast(JSON, dataclasses.asdict(times))
        for key, times in store.metrics.aggregate_times().items()
    }

    metrics = store.metrics._metrics.values()
    jsonified = map(dataclasses.asdict, metrics)

    return aggregated, list(jsonified)


def _write_metrics(
    store: Store[Any],
    aggregated_path: pathlib.Path,
    stats_path: pathlib.Path,
) -> None:
    metrics = _format_metrics(store)
    if metrics is None:
        return

    aggregated, individual = metrics
    if len(individual) == 0:
        return

    aggregated_path.parent.mkdir(parents=True, exist_ok=True)
    with open(aggregated_path, 'w') as f:
        json.dump(aggregated, f, indent=4)

    stats_path.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_path, 'a') as f:
        for stats in individual:
            json.dump(stats, f)
            f.write('\n')
