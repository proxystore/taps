# Synthetic Workflow

The synthetic workflow generates an arbitrary number of no-op sleep tasks that take and produce random data.
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

## Installation and Data

This application requires no additional dependencies besides TaPS and all data is generated randomly by the application.

## Example

The following example runs a bag of tasks workflow across four processes.
A maximum of four task can be running at any time, a total of 40 tasks will be submitted, each tasks sleeps for one second, and each task takes and produces 10 kB of data.

```bash
python -m taps.run --app synthetic \
    --app.structure bag --app.task-count 40 \
    --app.task-data-bytes 10000 --app.task-sleep 1 --app.bag-max-running 4 \
    --engine.executor process-pool --engine.executor.max-processes 4
```
