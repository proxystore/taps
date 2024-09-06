from __future__ import annotations

import pathlib

from taps.transformer import PickleFileTransformer
from taps.transformer import PickleFileTransformerConfig


def test_config() -> None:
    config = PickleFileTransformerConfig(file_dir='test')
    config.get_transformer()


def test_pickle_file_transformer(tmp_path: pathlib.Path) -> None:
    transformer = PickleFileTransformer(tmp_path)
    assert isinstance(repr(transformer), str)

    obj = [1, 2, 3]
    identifier = transformer.transform(obj)
    assert transformer.is_identifier(identifier)
    assert transformer.resolve(identifier) == obj

    transformer.close()
    assert not tmp_path.exists()
