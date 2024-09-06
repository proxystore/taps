# Task Data Management

Optimizing the transfer of task data and placement of tasks according to where data reside is a core feature of many task executors.
To support further research into data management, TaPS supports a plugin system for *data transformers*.

The [`Transformer`][taps.transformer.Transformer] protocol defines two methods: `transform()` which takes an object and returns an identifier, and `resolve()`, the inverse of `transform()`, which takes an identifier and returns the corresponding object.
Transformer implementations can implement object identifiers in any manner, provided identifier instances are serializable.
For example, an identifier could simply be a UUID corresponding to a database entry containing the serialized object.

A [`Filter`][taps.filter.Filter] is a callable object, e.g., a function, that takes an object as input and returns a boolean indicating if the object should be transformed by the data transformer.

The [`Engine`][taps.engine.Engine] uses the [`Transformer`][taps.transformer.Transformer] and [`Filter`][taps.filter.Filter] to transform the positional arguments, keyword arguments, and results of tasks before being sent to the [Task Executor](executor.md).
For example, every argument in the tuple of positional arguments which passes the filter check is transformed into an identifier using the data transformer.
Each task is encapsulated with a wrapper which will `resolve()` any arguments that were replaced with identifiers when the task begins executing.
The same occurs in reverse for a task's result.

## Transformer Types

As of writing, TaPS provides two transformer types.
By default, no transformer is configured.

### File Transformation

The [`PickleFileTransformer`][taps.transformer.PickleFileTransformer] pickles and writes objects to files in a specified directory.
The object identifiers are essentially the filepath of the pickle file.
This transformer can be configured like:
```toml title="Pickle File Transformer Config"
[engine.transformer]
name = "file"
file_dir = "./object-cache"
```
The `./object-cache` directory will contain any transformed objects and will be removed once a benchmark has completed.

### ProxyStore

The [`ProxyTransformer`][taps.transformer.ProxyTransformer] creates *proxies* of data using [ProxyStore](https://docs.proxystore.dev/){target=_blank}.
ProxyStore provides a pass-by-reference like model for distributed Python applications and supports a multitude of communication protocols including DAOS, Globus Transfer, Margo, Redis, UCX, and ZeroMQ.

Here are some example configurations.
The specific parameters will change change depending on specified `connector`.
See the [`ProxyTransformerConfig`][taps.transformer.ProxyTransformerConfig] for more information.

!!! note

    TaPS, by default, only installs the basic version of ProxyStore.
    It may be necessary to install ProxyStore with extra options to access certain features.
    See [ProxyStore → Installation → Extras Options](https://docs.proxystore.dev/latest/installation/#extras-options){target=_blank}.

**File System**

* Config file:
  ```toml
  [engine.transformer]
  name = "proxystore"

  [engine.transformer.connector]
  kind = "file"
  options = { store_dir = "./object-cache"}
  ```
* CLI arguments:
  ```bash
  --engine.transformer proxystore --engine.transformer.connector.kind file --engine.transformer.connector.options '{"store_dir": "./object-cache"}'
  ```

**Redis Server**

* Config file:
  ```toml
  [engine.transformer]
  name = "proxystore"

  [engine.transformer.connector]
  kind = "redis"
  options = { hostname = "localhost", port = 6379}
  ```
* CLI arguments:
  ```bash
  --engine.transformer proxystore --engine.transformer.connector.kind redis --engine.transformer.connector.options '{"hostname": "localhost", "port": 6379}'
  ```

## Adding Transformers

Transformer plugins are created by decorating a [`TransformerConfig`][taps.transformer.TransformerConfig] with [`@register('transformer')`][taps.plugins.register].

For example, the `FooTransformerConfig` for a `FooTransformer` might look like the following.
```python title="taps/transformer/_foo.py" linenums="1"
from typing import Literal

from pydantic import Field

from taps.plugins import register
from taps.transformer import TransformerConfig

@register('transformer')
class FooTransformerConfig(TransformerConfig):
    """Foo transformer configuration."""

    name: Literal['foo'] = Field(
        'foo',
        description='name of transformer type',
    )
    bar: int = Field(0, description='bar parameter')

    def get_transformer(self) -> FooTransformer:
        """Create a transformer from the configuration."""
        return FooTransformer(self.bar)
```
In order to ensure that the registration is performed, the `FooTransformerConfig` must be imported inside of `taps/transformer/__init__.py` and included in `__all__`.

## Filter Types

As mentioned above, a [`Filter`][taps.filter.Filter] determines what objects (i.e., task arguments and/or results) get passed to the transformer.
TaPS, by default, does not configure a [`Filter`][taps.filter.Filter].
This means that **all objects will be transformed** when a transformer is provided.

Other [`Filter`][taps.filter.Filter] types are provided to give fine-grained control over what objects get transformed.

* [`NeverFilter`][taps.filter.NeverFilter] (`#!toml name = "never"`): never transform objects even if a transformer is specified.
* [`ObjectSizeFilter`][taps.filter.ObjectSizeFilter] (`#!toml name = "object-size"`): checks if the size of an object (computed using [`sys.getsizeof()`][sys.getsizeof]) is greater than a minimum size and less than a maximum size.
* [`PickleSizeFilter`][taps.filter.PickleSizeFilter] (`#!toml name = "pickle-size"`): checks if the size of an object (computed using the size of the pickled object) is greater than a minimum size and less than a maximum size.
* [`ObjectTypeFilter`][taps.filter.ObjectTypeFilter] (`#!toml name = "object-type"`): checks if the object is of a certain type.

To use, for example, the [`ObjectSizeFilter`][taps.filter.ObjectSizeFilter], add the following to your configuration.
```toml title="Object Size Filter Config"
[engine.filter]
name = "object-size"
min_size = 1000
max_size = 1000000
```
This configuration will transform objects larger than 1 kB and smaller than 1 MB.

## Adding Filters

Filter plugins are created by decorating a [`FilterConfig`][taps.filter.FilterConfig] with [`@register('filter')`][taps.plugins.register].

For example, a `FooFilterConfig` look like the following.
```python title="taps/filters/_foo.py" linenums="1"
from typing import Literal

from pydantic import Field

from taps.plugins import register

@register('filter')
class FooFilterConfig(FilterConfig):
    """Foo filter configuration."""

    name: Literal['foo'] = Field('foo', description='name of filter type')
    bar: int = Field(0, description='bar parameter')

    def get_filter(self) -> Filter:
        """Create a filter from the configuration."""
        return FooFilter(self.bar)
```
In order to ensure that the registration is performed, the `FooFilterConfig` must be imported inside of `taps/filter/__init__.py` and included in `__all__`.
