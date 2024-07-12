from __future__ import annotations

import pytest

from taps.transformer import NullTransformer


def test_null_transformer() -> None:
    transformer = NullTransformer()
    obj = object()
    assert not transformer.is_identifier(obj)
    assert transformer.transform(obj) is obj
    with pytest.raises(NotImplementedError):
        transformer.resolve(obj)
    transformer.close()
