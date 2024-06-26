from __future__ import annotations

import contextlib
import logging
import pathlib
import random
import sys
from concurrent.futures import Executor
from typing import Any
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass


from taps.apps.configs.moldesign import MoldesignConfig
from taps.apps.failinject.failure_lib import FAILURE_LIB
from taps.engine import TaskFuture
from taps.engine.engine import Engine
from taps.filter import Filter
from taps.logging import APP_LOG_LEVEL
from taps.record import RecordLogger
from taps.transformer.protocol import Transformer

P = ParamSpec('P')
T = TypeVar('T')

logger = logging.getLogger(__name__)

# TODO: config dictionary for each workflow

config_dic = {
    'moldesign': MoldesignConfig(
        dataset='/home/szhou3/taps/data/moldesign/QM9-search.tsv',
        initial_count=4,
        search_count=16,
        batch_size=4,
    ),
}


def new_func(
    failure_rate: float,
    fail_task: Callable[P, T],
    success_task: Callable[P, T],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute failure task or true task according to failure rate."""
    if random.random() < failure_rate:
        return fail_task()
    else:
        return success_task(*args, **kwargs)


class FlawExecutor(Engine):
    """A wrapper over real Engine."""

    def __init__(
        self,
        failure_rate: float,
        failure_type: str,
        executor: Executor,
        *,
        data_filter: Filter | None = None,
        data_transformer: Transformer[Any] | None = None,
        record_logger: RecordLogger | None = None,
    ) -> None:
        self.failure_rate = failure_rate
        self.failure_type = failure_type

        super().__init__(
            executor,
            data_filter=data_filter,
            data_transformer=data_transformer,
            record_logger=record_logger,
        )

    def get_fail_task(self) -> Callable[..., None]:
        """Return fail task according to failure type."""
        if self.failure_type == 'random':
            return random.choice(list(FAILURE_LIB.values()))
        elif self.failure_type in FAILURE_LIB:
            return FAILURE_LIB[self.failure_type]
        else:
            # default: divide_zero error
            return FAILURE_LIB['divide_zero']

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> TaskFuture[T]:
        """Submit a task to the engine.

        Args:
            function: The function to be executed.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            TaskFuture[T]: A future object representing the task.
        """
        if (
            self.failure_type == 'dependency'
            and random.random() < self.failure_rate
        ):
            f1 = FAILURE_LIB['fails']
            f2 = FAILURE_LIB['depends']
            super().submit(f1)
            return super().submit(f2, f1)
        else:
            fail_task = self.get_fail_task()
            return super().submit(
                new_func,
                self.failure_rate,
                fail_task,
                function,
                *args,
                **kwargs,
            )


class FailinjectApp:
    """Failure injection application.

    Args:
        true_workflow: The target workflow
        failure_rate: The rate of failure
        failure_type: The type of failure
    """

    def __init__(
        self,
        true_workflow: str,
        failure_rate: float,
        failure_type: str,
    ) -> None:
        self.true_workflow = true_workflow
        self.failure_rate = failure_rate
        self.failure_type = failure_type

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the workflow.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        logger.log(
            APP_LOG_LEVEL,
            f'Try to inject {self.failure_type} with rate {self.failure_rate}',
        )

        # TODO: engine init twice, should only be once.
        # probably the cause of
        # "attempt to clean up DFK when it has already been cleaned-up"
        flaw_executor = FlawExecutor(
            self.failure_rate,
            self.failure_type,
            engine.executor,
            data_transformer=None,
            record_logger=None,
        )

        cfg = config_dic[self.true_workflow]
        # use cfg to create a true app instance
        app = cfg.get_app()

        with contextlib.closing(app), flaw_executor:
            app.run(engine=flaw_executor, run_dir=run_dir)
