# Quick Start

TaPS is a standardized framework for evaluating task-based execution frameworks and data management systems using a suite a real and synthetic scientific applications.

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

Applications can be executed from the CLI.
```bash
python -m taps.run {application} {args}
```

Checkout the [Application Guides](apps/index.md).
