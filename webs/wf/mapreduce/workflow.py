from __future__ import annotations

import logging
import os
import pathlib
import sys
from collections import Counter

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from webs.context import ContextManagerAddIn
from webs.executor.workflow import WorkflowExecutor
from webs.logging import WORK_LOG_LEVEL
from webs.wf.mapreduce.config import MapreduceWorkflowConfig
from webs.wf.mapreduce.utils import generate_author_lists_for_map_tasks
from webs.wf.mapreduce.utils import generate_paragraphs_for_map_tasks
from webs.workflow import register

logger = logging.getLogger(__name__)


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
                        WORK_LOG_LEVEL,
                        f"Error processing file '{file_path}': {e}",
                    )

    return word_count


@register()
class MapreduceWorkflow(ContextManagerAddIn):
    """Mapreduce workflow.

    Args:
        config: Workflow configuration.
    """

    name = 'mapreduce'
    config_type = MapreduceWorkflowConfig

    def __init__(self, config: MapreduceWorkflowConfig) -> None:
        self.config = config
        super().__init__()

    @classmethod
    def from_config(cls, config: MapreduceWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(config)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:
        """Run the MapReduce workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        logger.log(
            WORK_LOG_LEVEL,
            f'Running {self.name} workflow with config: {self.config}',
        )

        # Perform the map phase
        logger.log(
            WORK_LOG_LEVEL,
            'map phase starts',
        )
        map_counters: list[Counter[str]] = []

        if self.config.mode == 'enron':  # enron run mode
            author_lists = generate_author_lists_for_map_tasks(
                self.config.map_task_count,
                self.config.mail_dir,
            )
            map_task_inputs = zip(
                [self.config.mail_dir] * self.config.map_task_count,
                author_lists,
            )
            map_counters.extend(
                executor.map(
                    lambda args: map_function_for_enron_run_mode(*args),
                    map_task_inputs,
                ),
            )
        else:  # random run mode
            paragraphs = generate_paragraphs_for_map_tasks(
                self.config.map_task_count,
                self.config.word_count,
                self.config.word_len_min,
                self.config.word_len_max,
            )

            map_counters.extend(
                executor.map(map_function_for_random_run_mode, paragraphs),
            )

        logger.log(
            WORK_LOG_LEVEL,
            'map phase completes and reduce phase starts',
        )

        # Perform the reduce phase
        reduce_task = executor.submit(reduce_function, map_counters)

        # Examine the reduce phase result
        most_common_words = reduce_task.result().most_common(
            self.config.n_freq,
        )

        logger.log(
            WORK_LOG_LEVEL,
            f'{self.config.n_freq} most frequent words:',
        )
        for word, count in most_common_words:
            logger.log(
                WORK_LOG_LEVEL,
                f'{word:10s}: {count}',
            )
        logger.log(
            WORK_LOG_LEVEL,
            f'Total number of words {reduce_task.result().total()}',
        )
        # Save the reduce phase result
        output_file_path = os.path.join(run_dir, self.config.out)
        with open(output_file_path, 'w') as f:
            for word, count in most_common_words:
                f.write(f'{word:10s}: {count}\n')
        logger.log(
            WORK_LOG_LEVEL,
            f'Results are saved at: {output_file_path}',
        )
