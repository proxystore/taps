from __future__ import annotations

import pytest

from webs.wf.synthetic.utils import randbytes


@pytest.mark.parametrize('size', (0, 1, 10, 100))
def test_randbytes(size: int) -> None:
    b = randbytes(size)
    assert isinstance(b, bytes)
    assert len(b) == size
