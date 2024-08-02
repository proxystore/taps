from __future__ import annotations

from taps.engine.task import Task
from taps.engine.transform import TaskTransformer
from taps.filter import NullFilter
from taps.transformer import NullTransformer


def test_task_wrapper_call() -> None:
    def sum_(values: list[int], *, start: int = 0):
        return sum(values, start=start)

    task = Task(
        sum_,
        transformer=TaskTransformer(NullTransformer(), NullFilter()),
    )
    assert task([1, 2, 3], start=-6).result == 0
