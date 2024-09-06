from __future__ import annotations

import pickle
import sys
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import assert_type
else:  # pragma: <3.11 cover
    from typing_extensions import assert_type

import pytest

from taps.engine.task import Task
from taps.engine.task import task
from taps.engine.task import TaskResult
from taps.engine.transform import TaskTransformer

P = ParamSpec('P')
R = TypeVar('R')


def my_sum(values: list[int], *, start: int = 0) -> int:
    return sum(values, start=start)


def test_call_task() -> None:
    my_task = task(my_sum)
    assert isinstance(my_task, Task)

    result = my_task([1, 2, 3], start=-6, _transformer=TaskTransformer())

    assert isinstance(result, TaskResult)
    assert result.value == 0


def test_call_task_directly() -> None:
    my_task = task(my_sum)
    assert isinstance(my_task, Task)

    result = my_task([1, 2, 3], start=-6)

    assert isinstance(result, int)
    assert result == 0


@task
def non_pickleable_task() -> str:
    return 'foo'


@task()
def pickleable_task() -> str:
    return 'bar'


def test_decorator_usage() -> None:
    assert isinstance(non_pickleable_task, Task)
    assert non_pickleable_task() == 'foo'

    assert isinstance(pickleable_task, Task)
    assert pickleable_task() == 'bar'


def test_task_pickling() -> None:
    my_task = task(my_sum)
    pickled = pickle.dumps(my_task)
    result = pickle.loads(pickled)
    assert result([1, 2, 3], start=-6) == 0

    with pytest.raises(pickle.PicklingError, match='non_pickleable_task'):
        pickle.dumps(non_pickleable_task)

    pickled = pickle.dumps(pickleable_task)
    result = pickle.loads(pickled)
    assert result() == 'bar'


def test_task_return_type_overloading() -> None:
    my_task = task(my_sum)

    direct_result = my_task([1, 2, 3], start=-6)
    assert_type(direct_result, int)
    assert isinstance(direct_result, int)

    task_result = my_task([1, 2, 3], start=-6, _transformer=TaskTransformer())
    assert_type(task_result, TaskResult[int])
    assert isinstance(task_result, TaskResult)


def test_task_decorator_overloading() -> None:
    def foo(x: int) -> str:
        return str(x)

    decorator = task()
    assert_type(decorator, Callable[[Callable[P, R]], Task[P, R]])

    decorated: Task[[int], str] = decorator(foo)
    assert decorated(42) == '42'
    assert isinstance(decorated, Task)
    # The following is true if mypy succeeds because we assigned the output
    # to decorated: Task[[int], str] but mypy doesn't like this syntax.
    # assert_type(decorated, Task[[int], str])
