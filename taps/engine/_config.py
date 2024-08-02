from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from taps.engine._engine import Engine
from taps.executor import ExecutorConfig
from taps.executor import ProcessPoolConfig
from taps.filter import AllFilterConfig
from taps.filter import FilterConfig
from taps.record import JSONRecordLogger
from taps.transformer import NullTransformerConfig
from taps.transformer import TransformerConfig


class EngineConfig(BaseModel):
    """App engine configuration.

    Attributes:
        executor: Executor configuration.
        filter: Filter configuration.
        transformer: Transformer configuration.
        task_record_file_name: Name of line-delimited JSON file that task
            records are logged to.
    """

    executor: ExecutorConfig = Field(default_factory=ProcessPoolConfig)
    filter: FilterConfig = Field(default_factory=AllFilterConfig)
    transformer: TransformerConfig = Field(
        default_factory=NullTransformerConfig,
    )
    task_record_file_name: Optional[str] = Field('tasks.jsonl')  # noqa: UP007

    model_config: ConfigDict = ConfigDict(  # type: ignore[misc]
        extra='forbid',
        validate_default=True,
        validate_return=True,
    )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, EngineConfig):
            raise NotImplementedError(
                'EngineConfig equality is not implemented for '
                'non-EngineConfig types.',
            )

        return (
            self.executor == other.executor
            and self.filter == other.filter
            and self.transformer == other.transformer
            and self.task_record_file_name == other.task_record_file_name
        )

    def get_engine(self) -> Engine:
        """Create an engine from the configuration."""
        record_logger = (
            JSONRecordLogger(self.task_record_file_name)
            if self.task_record_file_name is not None
            else None
        )

        return Engine(
            executor=self.executor.get_executor(),
            filter_=self.filter.get_filter(),
            transformer=self.transformer.get_transformer(),
            record_logger=record_logger,
        )
