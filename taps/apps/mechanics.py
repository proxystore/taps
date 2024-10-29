from __future__ import annotations

import logging
import pathlib

from taps.engine import Engine
from taps.engine import task
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)


@task()
def print_message(message: str) -> None:
    """Print a message."""
    logger.log(APP_LOG_LEVEL, message)


class FoobarApp:
    """Foobar application.

    Args:
        message: Message to print.
        repeat: Number of times to repeat the message.
    """

    def __init__(self, message: str, repeat: int = 1) -> None:
        self.message = message
        self.repeat = repeat

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        for _ in range(self.repeat):
            task = engine.submit(print_message, self.message)
            task.result()  # Wait on task to finish

