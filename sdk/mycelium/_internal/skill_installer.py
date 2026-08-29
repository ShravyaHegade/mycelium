"""Offline installation of agent skills bundled with mycelium-runtime."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

SETUP_SKILL_NAME = "mycelium-setup"
DEFAULT_SKILL_CATALOG = Path(".agents/skills")


class SkillInstallError(ValueError):
    """Raised when a bundled skill cannot be installed safely."""


@dataclass(frozen=True)
class SkillInstallResult:
    """Result of installing one bundled skill into a catalog."""

    destination: Path
    changed: bool


def _setup_skill_resource():
    return resources.files("mycelium.skills").joinpath(SETUP_SKILL_NAME)


def _resource_files() -> dict[Path, bytes]:
    root = _setup_skill_resource()
    files: dict[Path, bytes] = {}

    def visit(node, relative: Path) -> None:
        if node.is_file():
            files[relative] = node.read_bytes()
            return
        if node.is_dir():
            for child in node.iterdir():
                visit(child, relative / child.name)

    visit(root, Path())
    required = {
        Path("SKILL.md"),
        Path("agents/openai.yaml"),
        Path("references/provider-reconciliation.md"),
        Path("references/tool-classification.md"),
    }
    missing = required.difference(files)
    if missing:
        names = ", ".join(sorted(str(path) for path in missing))
        raise SkillInstallError(f"bundled {SETUP_SKILL_NAME} skill is incomplete: {names}")
    return files


def _installed_files(destination: Path) -> dict[Path, bytes] | None:
    if not destination.is_dir() or destination.is_symlink():
        return None
    return {
        path.relative_to(destination): path.read_bytes()
        for path in destination.rglob("*")
        if path.is_file()
    }


def _remove_destination(destination: Path) -> None:
    if destination.is_symlink() or destination.is_file():
        destination.unlink()
    elif destination.exists():
        shutil.rmtree(destination)


def install_setup_skill(
    catalog: Path = DEFAULT_SKILL_CATALOG,
    *,
    force: bool = False,
) -> SkillInstallResult:
    """Install the bundled setup skill into ``catalog/mycelium-setup``.

    Installation is offline and idempotent. A different existing installation
    is never overwritten unless ``force=True`` is explicit.
    """

    catalog = Path(catalog).expanduser()
    destination = catalog / SETUP_SKILL_NAME
    bundled = _resource_files()
    installed = _installed_files(destination)
    if installed == bundled:
        return SkillInstallResult(destination=destination, changed=False)
    if destination.exists() or destination.is_symlink():
        if not force:
            raise SkillInstallError(
                f"{destination} already exists and differs from the bundled skill "
                "(use --force to replace it)"
            )

    catalog.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{SETUP_SKILL_NAME}-", dir=catalog))
    try:
        for relative, content in bundled.items():
            output = staging / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(content)
        if destination.exists() or destination.is_symlink():
            _remove_destination(destination)
        staging.replace(destination)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return SkillInstallResult(destination=destination, changed=True)


__all__ = [
    "DEFAULT_SKILL_CATALOG",
    "SETUP_SKILL_NAME",
    "SkillInstallError",
    "SkillInstallResult",
    "install_setup_skill",
]
