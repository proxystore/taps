from __future__ import annotations

import os
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


def generate_author_lists_for_map_tasks(
    task_count: int,
    mail_dir: str,
) -> list[list[str]]:
    """Generate task_count lists of email authors for the map tasks."""
    mail_dir = os.path.expanduser(mail_dir)

    # Get list of all immediate subdirectories
    author_dirs = [
        name
        for name in os.listdir(mail_dir)
        if os.path.isdir(os.path.join(mail_dir, name))
    ]

    # Split the list of directories into task_count sublists
    author_lists: list[list[str]] = [[] for _ in range(task_count)]
    for i, author in enumerate(author_dirs):
        author_lists[i % task_count].append(author)

    return author_lists
