from __future__ import annotations

import pathlib
from collections import Counter

import pytest

from taps.apps.mapreduce import generate_files
from taps.apps.mapreduce import generate_text
from taps.apps.mapreduce import generate_word
from taps.apps.mapreduce import map_task
from taps.apps.mapreduce import MapreduceApp
from taps.apps.mapreduce import reduce_task
from taps.engine import Engine


def test_map_task(tmp_path: pathlib.Path) -> None:
    n_files = 3
    files = [tmp_path / f'{i}.txt' for i in range(n_files)]

    for i, file in enumerate(files):
        with open(file, 'w') as f:
            f.write('\n'.join(str(x) for x in range(i + 1)))

    counter = map_task(*files)

    for i in range(n_files):
        assert counter[str(i)] == n_files - i


def test_reduce_task() -> None:
    counters = [Counter(), Counter(['1', '1', '1']), Counter(['1', '2', '3'])]
    reduced = reduce_task(*counters)

    assert reduced['1'] == 4  # noqa: PLR2004
    assert reduced['2'] == 1
    assert reduced['3'] == 1


def test_generate_word() -> None:
    word = generate_word(0, 0)
    assert isinstance(word, str)
    assert len(word) == 0

    min_length, max_length = 4, 8
    word = generate_word(min_length, min_length)
    assert isinstance(word, str)
    assert len(word) == min_length

    word = generate_word(min_length, max_length)
    assert isinstance(word, str)
    assert min_length <= len(word) <= max_length


def test_generate_text() -> None:
    min_length, max_length = 4, 8

    text = generate_text(0, min_length, max_length)
    assert isinstance(text, str)
    assert len(text) == 0

    text = generate_text(max_length, min_length, max_length)
    assert isinstance(text, str)
    words = text.split()
    assert len(words) == max_length
    assert all([min_length <= len(word) <= max_length for word in words])


def test_generate_files(tmp_path: pathlib.Path) -> None:
    n_files = 3
    words = 12

    files = generate_files(tmp_path, file_count=n_files, words_per_file=words)

    for file in files:
        with open(file) as f:
            assert len(f.read().split()) == words


def test_generate_files_nonempty_dir(tmp_path: pathlib.Path) -> None:
    (tmp_path / 'marker').touch()
    with pytest.raises(ValueError, match=f'Directory {tmp_path} is not empty'):
        generate_files(tmp_path, 0, 0)


def test_mapreduce_app_generate(
    app_engine: Engine,
    tmp_path: pathlib.Path,
) -> None:
    data_dir = tmp_path / 'data'
    run_dir = tmp_path / 'run'

    app = MapreduceApp(
        data_dir=data_dir,
        generate=True,
        generated_files=3,
        generated_words=12,
    )
    app.run(app_engine, run_dir)
    app.close()


def test_mapreduce_app_existing_files(
    app_engine: Engine,
    tmp_path: pathlib.Path,
) -> None:
    data_dir = tmp_path / 'data'
    run_dir = tmp_path / 'run'

    generate_files(data_dir, file_count=3, words_per_file=12)

    app = MapreduceApp(data_dir=data_dir, generate=False)
    app.run(app_engine, run_dir)
    app.close()
