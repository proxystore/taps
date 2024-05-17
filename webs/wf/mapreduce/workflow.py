from __future__ import annotations

import logging
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
from webs.wf.mapreduce.utils import generate_paragraphs_for_map_tasks
from webs.workflow import register

logger = logging.getLogger(__name__)


def map_function(paragraph: str) -> Counter[str]:
    """Map function to count words in a paragraph."""
    word_counts = Counter(paragraph.split())
    return word_counts


def reduce_function(counts_list: list[Counter[str]]) -> Counter[str]:
    """Reduce function to combine word counts."""
    total_counts: Counter[str] = Counter()
    for counts in counts_list:
        total_counts.update(counts)
    return total_counts


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

        # Generate paragraphs for map tasks
        paragraphs = generate_paragraphs_for_map_tasks(
            self.config.map_task_count,
            self.config.map_task_word_count,
            self.config.word_len_min,
            self.config.word_len_max,
        )

        # Perform the map phase
        map_counters: list[Counter[str]] = []
        map_counters.extend(executor.map(map_function, paragraphs))
        logger.log(WORK_LOG_LEVEL, 'map phase completes')

        # Perform the reduce phase
        reduce_task = executor.submit(reduce_function, map_counters)

        # Examine the reduce phase result
        logger.log(
            WORK_LOG_LEVEL,
            f'reduce task result: {reduce_task.result()}',
        )
