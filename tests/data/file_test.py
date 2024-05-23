from __future__ import annotations

import pathlib

from webs.data.file import PickleFileTransformer
from webs.data.file import PickleFileTransformerConfig


def test_config() -> None:
    config = PickleFileTransformerConfig(file_dir='test')
    config.get_transformer()


def test_pickle_file_transformer(tmp_path: pathlib.Path) -> None:
    transformer = PickleFileTransformer(tmp_path)

    obj = [1, 2, 3]
    identifier = transformer.transform(obj)
    assert transformer.is_identifier(identifier)
    assert transformer.resolve(identifier) == obj
