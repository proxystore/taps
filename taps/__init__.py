"""Task Performance Suite (TaPS).

TaPS is provides a common framework for writing task-based, distributed
applications supported through an extensive plugin system for running
applications with arbitrary task executors and data management systems.
The run CLI enables users to run performance experiments in a reproducible
manner.

![TaPS API Stack](../static/framework-stack.jpg)
> Overview of the abstraction stack with the TaPS framework.
> TaPS provides a framework for writing applications that can be executed
> with a variety of plugins.
"""

from __future__ import annotations

import importlib.metadata as importlib_metadata

__version__ = importlib_metadata.version('taps')
