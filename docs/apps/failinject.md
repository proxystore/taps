# FailInject

Inject failure into workflows.

## Installation

This application only requires TaPS to be installed.

## Data

Failure injection workflow itself does not require any dataset, but the data for the target workflow is needed.

## Example

```bash
python -m taps.run --app failinject \
    --app.true-workflow moldesign \
    --app.failure-rate 1 \
    --app.failure-type dependency \
    --engine.executor process-pool --engine.executor.max-processes 4
```
