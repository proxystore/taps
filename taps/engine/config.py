from __future__ import annotations

from pydantic import BaseModel
from pydantic import Field

from taps.engine.engine import AppEngine
from taps.executor import ExecutorConfig
from taps.executor import ProcessPoolConfig
from taps.filter import FilterConfig
from taps.filter import NullFilterConfig
from taps.record import JSONRecordLogger
from taps.transformer import DataTransformerConfig
from taps.transformer import NullTransformerConfig


class AppEngineConfig(BaseModel):
    """App engine configuration.

    Attributes:
        executor: Executor configuration.
        filter: Filter configuration.
        transformer: Transformer configuration.
        task_record_file_name: Name of line-delimited JSON file that task
            records are logged to.
    """

    executor: ExecutorConfig = Field(default_factory=ProcessPoolConfig)
    filter: FilterConfig = Field(default_factory=NullFilterConfig)
    transformer: DataTransformerConfig = Field(
        default_factory=NullTransformerConfig,
    )
    task_record_file_name: str | None = Field('tasks.jsonl')

    def get_engine(self) -> AppEngine:
        """Create an engine from the configuration."""
        record_logger = (
            JSONRecordLogger(self.task_record_file_name)
            if self.task_record_file_name is not None
            else None
        )

        return AppEngine(
            executor=self.executor.get_executor(),
            data_filter=self.filter.get_filter(),
            data_transformer=self.transformer.get_transformer(),
            record_logger=record_logger,
        )
