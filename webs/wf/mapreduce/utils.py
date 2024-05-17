from __future__ import annotations

import random
import string


def generate_word(word_min_len: int, word_max_len: int) -> str:
    """Generate a random word."""
    length = random.randint(word_min_len, word_max_len)
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def generate_paragraph(
    word_count: int,
    word_min_len: int,
    word_max_len: int,
) -> str:
    """Generate a paragraph with the specified number of words."""
    words = [
        generate_word(word_min_len, word_max_len) for _ in range(word_count)
    ]
    return ' '.join(words)


def generate_paragraphs_for_map_tasks(
    task_count: int,
    word_count: int,
    word_min_len: int,
    word_max_len: int,
) -> list[str]:
    """Generate task_count paragraphs for the map tasks."""
    paragraphs = [
        generate_paragraph(word_count, word_min_len, word_max_len)
        for _ in range(task_count)
    ]
    return paragraphs
