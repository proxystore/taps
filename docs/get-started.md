# Quick Start

The Workflow Execution Benchmark Suite (WEBS) provides a set of standard computational workflows that can be executed with a variety of execution engines.

## Installation

```bash
git clone https://github.com/proxystore/taps
cd taps
python -m venv venv
. venv/bin/activate
pip install -e .
```

Documentation on installing for local development is provided in [Contributing](contributing/index.md).

## Usage

```bash
python -m webs.run {workflow-name} {args}
```
