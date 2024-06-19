from __future__ import annotations

import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator

from taps.engine.engine import AppEngine
from taps.executor.config import ExecutorConfigs
from taps.filter.config import FilterConfigs
from taps.record import JSONRecordLogger
from taps.transformer.config import DataTransformerConfigs


class AppEngineConfig(BaseModel):
    executor: str = Field('process-pool')
    filter: str | None = Field(None)
    transformer: str | None = Field(None)
    task_record_file_name: str | None = Field('tasks.json')

    executors: ExecutorConfigs = Field(default_factory=ExecutorConfigs)
    filters: FilterConfigs = Field(default_factory=FilterConfigs)
    transformers: DataTransformerConfigs = Field(
        default_factory=DataTransformerConfigs,
    )

    @model_validator(mode='after')
    def _validate_options(self) -> Self:
        options = [
            ('executor', self.executor, self.executors),
            ('filter', self.filter, self.filters),
            ('transformer', self.transformer, self.transformers),
        ]

        for option, value, configs in options:
            if value is None:
                continue
            value_attr = value.replace('-', '_')
            if not hasattr(configs, value_attr):
                raise ValueError(f'No {option} named {value}.')

        return self

    def get_engine(self) -> AppEngine:
        executor = self.executors.get_executor(self.executor)
        filter_ = self.filters.get_filter(self.filter)
        transformer = self.transformers.get_transformer(self.transformer)
        record_logger = (
            JSONRecordLogger(self.task_record_file_name)
            if self.task_record_file_name is not None
            else None
        )

        return AppEngine(
            executor=executor,
            data_filter=filter_,
            data_transformer=transformer,
            record_logger=record_logger,
        )
