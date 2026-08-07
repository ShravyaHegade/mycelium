"""Package version must track pyproject.toml (no hardcoded drift)."""

from __future__ import annotations

import re
from pathlib import Path

import mycelium


def test_version_matches_pyproject() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    match = re.search(
        r'(?m)^version\s*=\s*["\']([^"\']+)["\']\s*$',
        text,
    )
    assert match is not None, "version missing from pyproject.toml"
    assert mycelium.__version__ == match.group(1)
