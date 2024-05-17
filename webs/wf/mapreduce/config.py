from __future__ import annotations

from pydantic import Field

from webs.config import Config


class MapreduceWorkflowConfig(Config):
    """Mapreduce workflow configuration."""

    word_len_min: int = Field(1, description='minimum word length (inclusive)')
    word_len_max: int = Field(1, description='maximum word length (inclusive)')
    map_task_word_count: int = Field(description='words per map task')
    map_task_count: int = Field(description='number of map tasks')
