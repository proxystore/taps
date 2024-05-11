from __future__ import annotations

import pathlib

from webs.data.file import PickleFileTransformer


def test_pickle_file_transformer(tmp_path: pathlib.Path) -> None:
    transformer = PickleFileTransformer(tmp_path)

    obj = [1, 2, 3]
    identifier = transformer.transform(obj)
    assert transformer.is_identifier(identifier)
    assert transformer.resolve(identifier) == obj

    assert len(list(tmp_path.glob('*'))) == 0
