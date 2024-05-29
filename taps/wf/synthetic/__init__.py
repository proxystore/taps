"""Synthetic workflow.

The synthetic workflow generates an arbitrary number of no-op sleep tasks
that take and produce random data.

The following arguments are supported:

* `--structure`
* `--task-count`
* `--task-data-bytes`
* `--task-sleep`
* `--bag-max-running`

The workflow supports four workflow structures:

* `bag`: Executes a "bag-of-tasks" where `task-count` tasks are executed. At
  most `bag-max-running` will be running at any given time. This is useful
  for testing scalability.
* `diamond`: Executes a diamond workflow where the output of an initial task
  is given to `task-count` intermediate tasks, executed in parallel, and
  the outputs of the intermediate tasks are aggregated in a single, final
  task.
* `reduce`: Executes `task-count` independent tasks in parallel and a single
  reduce task that takes the output of all of the independent tasks.
* `sequential`: Executes a chain of `task-count` tasks where each subsequent
  task depends on the output data of the prior. There is no parallelism, but
  is useful for evaluating task and data overheads.

## Running

Note that all arguments are required regardless of the `--structure`.

```bash
python -m taps.run synthetic --executor process-pool --max-processes 4 --structure bag --task-count 40 --task-data-bytes 10000 --task-sleep 1 --bag-max-running 4
```
"""  # noqa: E501

from __future__ import annotations
