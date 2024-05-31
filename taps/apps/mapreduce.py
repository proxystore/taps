from __future__ import annotations

import logging
import os
import pathlib
import random
import string
from collections import Counter

from taps.engine import AppEngine
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)


def generate_word(word_min_len: int, word_max_len: int) -> str:
    """Generate a random word."""
    length = random.randint(word_min_len, word_max_len)
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def generate_paragraph(
    word_count: int,
    word_min_len: int,
    word_max_len: int,
) -> str:
    """Generate a paragraph with the specified number of words."""
    words = [
        generate_word(word_min_len, word_max_len) for _ in range(word_count)
    ]
    return ' '.join(words)


def generate_paragraphs_for_map_tasks(
    task_count: int,
    word_count: int,
    word_min_len: int,
    word_max_len: int,
) -> list[str]:
    """Generate task_count paragraphs for the map tasks."""
    paragraphs = [
        generate_paragraph(word_count, word_min_len, word_max_len)
        for _ in range(task_count)
    ]
    return paragraphs


def generate_author_lists_for_map_tasks(
    task_count: int,
    mail_dir: str,
) -> list[list[str]]:
    """Generate task_count lists of email authors for the map tasks."""
    mail_dir = os.path.expanduser(mail_dir)

    # Get list of all immediate subdirectories
    author_dirs = [
        name
        for name in os.listdir(mail_dir)
        if os.path.isdir(os.path.join(mail_dir, name))
    ]

    # Split the list of directories into task_count sublists
    author_lists: list[list[str]] = [[] for _ in range(task_count)]
    for i, author in enumerate(author_dirs):
        author_lists[i % task_count].append(author)

    return author_lists


def map_function_for_random_run_mode(paragraph: str) -> Counter[str]:
    """Map function to count words in a paragraph."""
    word_counts = Counter(paragraph.split())
    return word_counts


def reduce_function(counts_list: list[Counter[str]]) -> Counter[str]:
    """Reduce function to combine word counts."""
    total_counts: Counter[str] = Counter()
    for counts in counts_list:
        total_counts.update(counts)
    return total_counts


def map_function_for_enron_run_mode(
    mail_dir: str,
    authors: list[str],
) -> Counter[str]:
    """Count words in all files under mail_dir/author for each author."""
    mail_dir = os.path.expanduser(mail_dir)
    word_count: Counter[str] = Counter()

    for author in authors:
        author_dir = os.path.join(mail_dir, author)

        # Walk through all files in the author's directory
        for root, _, files in os.walk(author_dir):
            for file in files:
                file_path = os.path.join(root, file)

                try:  # Count words in each file
                    with open(file_path, errors='ignore') as f:
                        for line in f:
                            words = line.split()
                            word_count.update(words)
                except Exception as e:
                    logger.log(
                        APP_LOG_LEVEL,
                        f"Error processing file '{file_path}': {e}",
                    )

    return word_count


def _map_function_for_enron_run_mode(
    data: tuple[str, list[str]],
) -> Counter[str]:
    return map_function_for_enron_run_mode(data[0], data[1])


class MapreduceApp:
    """Mapreduce application.

    Args:
        mode: Run mode (enron or random).
        map_task_count: Number of map tasks.
        word_count: Words per map task in random mode.
        word_len_min: Min word length in random mode.
        word_len_max: Max word length in random mode.
        mail_dir: Path to maildir in enron mode.
        n_freq: Save the n most frequent words.
        out: Output file name for most frequent words.
    """

    def __init__(
        self,
        mode: str,
        map_task_count: int,
        word_count: int = 500,
        word_len_min: int = 1,
        word_len_max: int = 10,
        mail_dir: str = '~/maildir',
        n_freq: int = 10,
        out: str = 'output.txt',
    ) -> None:
        self.mode = mode
        self.map_task_count = map_task_count
        self.word_count = word_count
        self.word_len_min = word_len_min
        self.word_len_max = word_len_max
        self.mail_dir = mail_dir
        self.n_freq = n_freq
        self.out = out

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: AppEngine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        # Perform the map phase
        logger.log(APP_LOG_LEVEL, 'Starting map phase')
        map_counters: list[Counter[str]] = []

        if self.mode == 'enron':
            author_lists = generate_author_lists_for_map_tasks(
                self.map_task_count,
                self.mail_dir,
            )
            map_task_inputs = zip(
                [self.mail_dir] * self.map_task_count,
                author_lists,
            )
            map_counters.extend(
                engine.map(
                    _map_function_for_enron_run_mode,
                    map_task_inputs,
                ),
            )
        else:
            paragraphs = generate_paragraphs_for_map_tasks(
                self.map_task_count,
                self.word_count,
                self.word_len_min,
                self.word_len_max,
            )

            map_counters.extend(
                engine.map(map_function_for_random_run_mode, paragraphs),
            )

        logger.log(
            APP_LOG_LEVEL,
            'Map phase completed. Starting reduce phase',
        )

        # Perform the reduce phase
        reduce_task = engine.submit(reduce_function, map_counters)

        # Examine the reduce phase result
        most_common_words = reduce_task.result().most_common(
            self.n_freq,
        )

        logger.log(
            APP_LOG_LEVEL,
            f'{self.n_freq} most frequent words:',
        )
        for word, count in most_common_words:
            logger.log(APP_LOG_LEVEL, f'{word:10s}: {count}')
        logger.log(
            APP_LOG_LEVEL,
            f'Total number of words {sum(reduce_task.result().values())}',
        )
        # Save the reduce phase result
        output_file_path = os.path.join(run_dir, self.out)
        with open(output_file_path, 'w') as f:
            for word, count in most_common_words:
                f.write(f'{word},{count}\n')

        logger.log(APP_LOG_LEVEL, f'Results saved to: {output_file_path}')
