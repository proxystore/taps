from __future__ import annotations

from taps.engine.task import Task
from taps.engine.task import task
from taps.engine.task import TaskResult
from taps.engine.transform import TaskTransformer
from taps.filter import NullFilter
from taps.transformer import NullTransformer


def my_sum(values: list[int], *, start: int = 0) -> int:
    return sum(values, start=start)


def test_call_task() -> None:
    task = Task(my_sum)
    transformer = TaskTransformer(NullTransformer(), NullFilter())
    result = task([1, 2, 3], start=-6, _transformer=transformer)

    assert isinstance(result, TaskResult)
    assert result.value == 0


def test_call_task_directly() -> None:
    task = Task(my_sum)
    result = task([1, 2, 3], start=-6)
    assert isinstance(result, int)
    assert result == 0


def test_decorator() -> None:
    @task
    def foo1() -> None:
        pass

    assert isinstance(foo1, Task)
    foo1()

    @task()
    def foo2() -> None:
        pass

    assert isinstance(foo1, Task)
    foo2()
