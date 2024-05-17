from __future__ import annotations

from pydantic import Field

from webs.config import Config


class MapreduceWorkflowConfig(Config):
    """Mapreduce workflow configuration."""

    # Required arguments
    mode: str = Field(description='"random" or "enron" run mode')
    map_task_count: int = Field(description='number of map tasks')

    # For the random run mode
    word_count: int = Field(500, description='[random] words per map task')
    word_len_min: int = Field(1, description='[random] min word length')
    word_len_max: int = Field(1, description='[random] max word length')

    # For the enron run mode
    mail_dir: str = Field('~/maildir/', description='[enron] path to maildir')

    # To Save reduce task result
    n_freq: int = Field(10, description='how many most frequent words to save')
    out: str = Field('output.txt', description='output file name')
