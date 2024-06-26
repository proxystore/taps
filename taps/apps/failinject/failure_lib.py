from __future__ import annotations

import os
import random
import signal
from typing import Any
from typing import Callable
from typing import TypeVar

import psutil

T = TypeVar('T')


def fails() -> None:
    """Real error in dependency failure."""
    raise ValueError('Deliberate failure')


def depends(parent: Any) -> None:
    """Task that depends on fails()."""
    parent()


def divide_zero() -> None:
    """Raise divide by zero error."""
    res = 100 / 0
    print(res)


def environment() -> None:
    """Raise package not exist error."""
    raise ImportError


def manager_kill() -> None:
    """Kill father process of this worker, i.e. manager process."""
    current_pid = os.getpid()
    current_process = psutil.Process(current_pid)
    parent_process = current_process.parent()

    if parent_process:
        parent_pid = parent_process.pid
        print(f'Killing Manager with PID: {parent_pid}')
        try:
            os.kill(parent_pid, signal.SIGTERM)
            print(f'Parent process {parent_pid} terminated.')
        except psutil.NoSuchProcess:
            print(f'Parent process {parent_pid} does not exist.')
        except psutil.AccessDenied:
            print(f'No permission to terminate parent process {parent_pid}.')
    else:
        print('No parent process found.')


def memory() -> None:
    """Raise MemoryError by running of out memory."""
    huge_memory_list = []
    while True:
        huge_memory_list.append('A' * 1024 * 1024 * 100)


def node_kill() -> None:
    """Kill all the processes in this node to simulate node shut down."""
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
        ) as e:
            print(f'raise exception {e} in node kill')

    psutil.wait_procs(psutil.process_iter(), timeout=3, callback=None)
    print('node killed')


def ulimit() -> None:
    """Open more files than allowed to simulate ulimit error."""
    limit = 548001
    handles = []
    try:
        for i in range(limit):
            handles.append(open(f'/tmp/tempfile_{i}.txt', 'w'))  # noqa: SIM115

        print(f'Opened {limit} files successfully')
    finally:
        for handle in handles:
            handle.close()


def walltime() -> None:
    """Raise walltime error by sleeping longer than allowed."""
    import time

    while True:
        time.sleep(60)


def worker_kill() -> None:
    """Kill a random process in current node."""
    current_pid = os.getpid()
    parent_pid = os.getppid()

    all_processes_pid = []
    for proc in psutil.process_iter(attrs=['pid', 'username']):
        pid = proc.info['pid']

        if pid in (current_pid, parent_pid):
            continue

        all_processes_pid.append(pid)

    process_to_kill = random.choice(all_processes_pid)
    try:
        p = psutil.Process(process_to_kill)
        p.terminate()
        print(f'Killed process {process_to_kill}')
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        print(f'Can not kill {process_to_kill}')


FAILURE_LIB: dict[str, Callable[[], Any] | Callable[[Any], Any]] = {
    'depends': depends,
    'divide_zero': divide_zero,
    'environment': environment,
    'fails': fails,
    'manager_kill': manager_kill,
    'memory': memory,
    'node_kill': node_kill,
    'ulimit': ulimit,
    'walltime': walltime,
    'worker_kill': worker_kill,
}
