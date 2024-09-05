from __future__ import annotations

import logging
import math
import pathlib
import random
import shutil
import string
from collections import Counter
from typing import Generator
from typing import TypeVar

from taps.engine import Engine
from taps.engine import task
from taps.logging import APP_LOG_LEVEL

T = TypeVar('T')

logger = logging.getLogger(__name__)


@task()
def map_task(*files: pathlib.Path) -> Counter[str]:
    """Count words in files."""
    counts: Counter[str] = Counter()
    for file in files:
        with open(file, errors='ignore') as f:
            for line in f:
                counts.update(line.split())
    return counts


@task()
def reduce_task(*counts: Counter[str]) -> Counter[str]:
    """Combine word counts."""
    total: Counter[str] = Counter()
    for count in counts:
        total.update(count)
    return total


def generate_word(word_min_length: int, word_max_length: int) -> str:
    """Generate a random word."""
    length = random.randint(word_min_length, word_max_length)
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def generate_text(
    word_count: int,
    word_min_length: int,
    word_max_length: int,
) -> str:
    """Generate a paragraph with the specified number of words."""
    return ' '.join(
        generate_word(word_min_length, word_max_length)
        for _ in range(word_count)
    )


def generate_files(
    directory: pathlib.Path,
    file_count: int,
    words_per_file: int,
    *,
    min_word_length: int = 2,
    max_word_length: int = 10,
) -> list[pathlib.Path]:
    """Generate text files with random text.

    Args:
        directory: Directory to write the files to.
        file_count: Number of files to generate.
        words_per_file: Number of words per file.
        min_word_length: Minimum character length of randomly generated words.
        max_word_length: Maximum character length of randomly generated words.

    Returns:
        List of generated files.

    Raises:
        ValueError: if `directory` is not empty.
    """
    if directory.is_dir() and any(directory.iterdir()):
        raise ValueError(f'Directory {directory} is not empty.')

    directory.mkdir(parents=True, exist_ok=True)

    files = []
    for i in range(file_count):
        filename = (directory / f'{i}.txt').resolve()
        text = generate_text(words_per_file, min_word_length, max_word_length)
        with open(filename, 'w') as f:
            f.write(text)
        files.append(filename)

    return files


def _chunkify(iterable: list[T], n: int) -> Generator[list[T], None, None]:
    chunk_size = math.ceil(len(iterable) / n)
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : min(i + chunk_size, len(iterable) - 1)]


class MapreduceApp:
    """Mapreduce application.

    Args:
        data_dir: Text file directory. Either contains existing text files
            (including in subdirectories) or will be used to store the
            randomly generated files.
        map_tasks: Number of map tasks. If `None`, one map task is generated
            per text file. Otherwise, files are evenly distributed across the
            map tasks.
        generate: Generate random text files for the application.
        generated_files: Number of text files to generate.
        generated_words: Number of words per text file to generate.
    """

    def __init__(
        self,
        data_dir: pathlib.Path,
        map_tasks: int | None = None,
        generate: bool = False,
        generated_files: int = 10,
        generated_words: int = 10_000,
    ) -> None:
        self.generate = generate
        self.data_dir = data_dir

        if self.generate:
            files = generate_files(data_dir, generated_files, generated_words)
            logger.log(APP_LOG_LEVEL, f'Generated {len(files)} in {data_dir}')
        else:
            files = [f for f in data_dir.glob('**/*') if f.is_file()]
            logger.log(APP_LOG_LEVEL, f'Found {len(files)} in {data_dir}')

        self.files = files
        self.map_tasks = len(self.files) if map_tasks is None else map_tasks

    def close(self) -> None:
        """Close the application."""
        if self.generate:
            shutil.rmtree(self.data_dir)
            logger.log(
                APP_LOG_LEVEL,
                f'Removed generated files in {self.data_dir}',
            )

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        map_futures = [
            engine.submit(map_task, *batch)
            for batch in _chunkify(self.files, self.map_tasks)
        ]
        logger.log(
            APP_LOG_LEVEL,
            f'Submitted {len(map_futures):,} map tasks over '
            f'{len(self.files):,} input files',
        )

        reduce_future = engine.submit(reduce_task, *map_futures)
        logger.log(APP_LOG_LEVEL, 'Submitted reduce task')

        word_counts = reduce_future.result()
        logger.log(APP_LOG_LEVEL, 'Reduce task finished')

        most_common_words = word_counts.most_common(10)
        logger.log(
            APP_LOG_LEVEL,
            f'{len(most_common_words)} most frequent words:',
        )
        for word, count in most_common_words:
            logger.log(APP_LOG_LEVEL, f'{word} ({count:,})')

        logger.log(
            APP_LOG_LEVEL,
            f'Total number of words: {sum(word_counts.values()):,}',
        )
