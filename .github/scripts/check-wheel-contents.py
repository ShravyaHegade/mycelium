"""Fail when a wheel omits the official Mycelium setup skill."""

from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile

REQUIRED = {
    "mycelium/skills/mycelium-setup/SKILL.md",
    "mycelium/skills/mycelium-setup/agents/openai.yaml",
    "mycelium/skills/mycelium-setup/references/provider-reconciliation.md",
    "mycelium/skills/mycelium-setup/references/tool-classification.md",
}


def check(wheel: Path) -> None:
    with ZipFile(wheel) as archive:
        names = set(archive.namelist())
        missing = REQUIRED.difference(names)
        empty = {name for name in REQUIRED.intersection(names) if not archive.read(name)}
    if missing or empty:
        details = []
        if missing:
            details.append(f"missing: {', '.join(sorted(missing))}")
        if empty:
            details.append(f"empty: {', '.join(sorted(empty))}")
        raise SystemExit(f"{wheel}: bundled setup skill check failed ({'; '.join(details)})")
    print(f"{wheel}: bundled setup skill is complete")


def main(argv: list[str]) -> None:
    if not argv:
        raise SystemExit("usage: check-wheel-contents.py WHEEL [WHEEL ...]")
    for value in argv:
        check(Path(value))


if __name__ == "__main__":
    main(sys.argv[1:])
