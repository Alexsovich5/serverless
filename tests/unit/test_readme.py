import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
README = os.path.join(ROOT, 'README.md')

FORBIDDEN = [
    'ACORIA',
    '**Role**',
    'Role:',
    '**Organization**',
    'Organization:',
    'Developed during',
    '99.9',
    '70%',
    '10,000',
    'Status-Complete',
    'img.shields.io',
]


def read_readme():
    with open(README) as handle:
        return handle.read()


def layout_block(text):
    match = re.search(r'^## Layout\s*\n+```[^\n]*\n(.*?)^```', text, re.M | re.S)
    assert match, 'README has no fenced Layout block'
    return match.group(1)


def layout_paths(block):
    """Turn an indented tree (two spaces per level, directories end in '/') into paths."""
    stack = []
    paths = []
    for line in block.splitlines():
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(' '))
        assert indent % 2 == 0, 'odd indentation in Layout: {!r}'.format(line)
        depth = indent // 2
        assert depth <= len(stack), 'Layout skips a level: {!r}'.format(line)
        stack = stack[:depth]
        name = line.strip()
        if name.endswith('/'):
            stack.append(name.rstrip('/'))
        paths.append(os.path.join(*(stack[:depth] + [name.rstrip('/')])))
    return paths


def test_layout_paths_parse_nested_tree():
    block = 'a/\n  b/\n    c.py\n  d.py\ne.txt\n'
    assert layout_paths(block) == ['a', 'a/b', 'a/b/c.py', 'a/d.py', 'e.txt']


def test_layout_lists_files():
    paths = layout_paths(layout_block(read_readme()))
    assert 'functions/api_handler.py' in paths
    assert 'tests/unit/test_readme.py' in paths


def test_every_layout_path_exists():
    missing = [path for path in layout_paths(layout_block(read_readme()))
               if not os.path.exists(os.path.join(ROOT, path))]
    assert missing == []


@pytest.mark.parametrize('phrase', FORBIDDEN)
def test_readme_has_no_unsupported_claims(phrase):
    assert phrase not in read_readme()


def test_readme_names_the_emulators():
    text = read_readme()
    assert 'LocalStack' in text
    assert 'moto' in text


def test_template_placeholders_and_comment_removed():
    text = read_readme()
    assert '{{' not in text
    assert '<!--' not in text
