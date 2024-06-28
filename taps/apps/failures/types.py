from __future__ import annotations

import enum
import logging
import os
import signal
import tempfile

import psutil

logger = logging.getLogger(__name__)


class FailureType(enum.Enum):
    """Failure types."""

    DEPENDENCY = 'dependency'
    FAILURE = 'failure'
    IMPORT = 'import'
    MANAGER_KILLED = 'manager-killed'
    MEMORY = 'memory'
    NODE_KILLED = 'node-killed'
    RANDOM = 'random'
    TIMEOUT = 'timeout'
    ULIMIT = 'ulimit'
    WORKER_KILLED = 'worker-killed'
    ZERO_DIVISION = 'zero-division'


def exception_failure() -> None:
    """Raise an exception."""
    raise Exception('Failure injection error.')


def import_failure() -> None:
    """Simulate an import error due to a bad environment."""
    raise ImportError('Failure injection error.')


def manager_killed_failure() -> None:
    """Kill the parent process (i.e., the manager)."""
    current_pid = os.getpid()
    current_process = psutil.Process(current_pid)
    parent_process = current_process.parent()

    if parent_process is None:
        logger.warning(
            f'Task process (pid={current_process} has no parent process',
        )
        return

    parent_pid = parent_process.pid
    logger.info(f'Killing manager parent process (pid={parent_pid})')
    try:
        os.kill(parent_pid, signal.SIGTERM)
        logger.info(f'Parent process terminated (pid={parent_pid})')
    except psutil.NoSuchProcess:
        logger.exception('Parent process does not exist')
    except psutil.AccessDenied:
        logger.exception(
            'Insufficient permission to terminate parent process',
        )


def memory_failure() -> None:
    """Force an out of memory error."""
    huge_memory_list = []
    while True:
        huge_memory_list.append('x' * (1024**3))


def node_killed_failure() -> None:
    """Kill other processes in the node to simulate a node failure.

    Warning:
        This is a very dangerous function. It will kill random processes
        on the node. Do not run this function in a process with sudo
        privileges.
    """
    current_pid = os.getpid()

    for proc in psutil.process_iter(attrs=['pid', 'name']):
        pid = proc.info['pid']
        if pid == current_pid:
            continue
        try:
            p = psutil.Process(pid)
            p.terminate()
        except (
            psutil.NoSuchProcess,
            psutil.AccessDenied,
            psutil.ZombieProcess,
        ):
            logger.exception(f'Exception when killing process (pid={pid})')

    psutil.wait_procs(psutil.process_iter(), timeout=3, callback=None)


def worker_killed_failure() -> None:
    """Kill the current process."""
    pid = os.getpid()
    try:
        psutil.Process(pid).terminate()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        logger.exception(f'Failed to kill current process (pid={pid})')


def timeout_failure() -> None:
    """Sleep forever to force walltime or timeout error."""
    import time

    while True:
        time.sleep(60)


def ulimit_failure() -> None:
    """Open 1M files to simulate ulimit exceeded error."""
    limit = 1_000_000
    handles = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            for i in range(limit):
                file = os.path.join(tmp_dir, f'{i}.txt')
                handles.append(open(file, 'w'))  # noqa: SIM115
        finally:
            for handle in handles:
                handle.close()


def zero_division_failure() -> None:
    """Raise divide by zero error."""
    raise ZeroDivisionError('Failure injection error.')


FAILURE_FUNCTIONS = {
    FailureType.FAILURE: exception_failure,
    FailureType.IMPORT: import_failure,
    FailureType.MANAGER_KILLED: manager_killed_failure,
    FailureType.MEMORY: memory_failure,
    FailureType.NODE_KILLED: node_killed_failure,
    FailureType.TIMEOUT: timeout_failure,
    FailureType.ULIMIT: ulimit_failure,
    FailureType.WORKER_KILLED: worker_killed_failure,
    FailureType.ZERO_DIVISION: zero_division_failure,
}
