"""Verify wheel and sdist copies of the bundled setup skill byte-for-byte."""

from __future__ import annotations

import argparse
import tarfile
from pathlib import Path, PurePosixPath
from zipfile import BadZipFile, ZipFile

SKILL_PARTS = ("mycelium", "skills", "mycelium-setup")
DEFAULT_CANONICAL = Path(__file__).parents[2] / ".agents/skills/mycelium-setup"


def _canonical_files(root: Path) -> dict[PurePosixPath, bytes]:
    files = {
        PurePosixPath(path.relative_to(root).as_posix()): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    if not files:
        raise SystemExit(f"canonical skill is empty or missing: {root}")
    return files


def _skill_relative(name: str) -> PurePosixPath | None:
    path = PurePosixPath(name)
    parts = path.parts
    for index in range(len(parts) - len(SKILL_PARTS) + 1):
        if parts[index : index + len(SKILL_PARTS)] == SKILL_PARTS:
            relative = parts[index + len(SKILL_PARTS) :]
            return PurePosixPath(*relative) if relative else None
    return None


def _wheel_files(artifact: Path) -> dict[PurePosixPath, bytes]:
    files: dict[PurePosixPath, bytes] = {}
    try:
        with ZipFile(artifact) as archive:
            corrupt = archive.testzip()
            if corrupt is not None:
                raise SystemExit(f"{artifact}: corrupt wheel member: {corrupt}")
            for member in archive.infolist():
                relative = _skill_relative(member.filename)
                if relative is None or member.is_dir():
                    continue
                if relative in files:
                    raise SystemExit(f"{artifact}: duplicate skill member: {relative}")
                files[relative] = archive.read(member)
    except BadZipFile as exc:
        raise SystemExit(f"{artifact}: invalid wheel: {exc}") from exc
    return files


def _sdist_files(artifact: Path) -> dict[PurePosixPath, bytes]:
    files: dict[PurePosixPath, bytes] = {}
    try:
        with tarfile.open(artifact, mode="r:gz") as archive:
            for member in archive.getmembers():
                relative = _skill_relative(member.name)
                if relative is None or not member.isfile():
                    continue
                if relative in files:
                    raise SystemExit(f"{artifact}: duplicate skill member: {relative}")
                source = archive.extractfile(member)
                if source is None:
                    raise SystemExit(f"{artifact}: unreadable skill member: {relative}")
                files[relative] = source.read()
    except tarfile.TarError as exc:
        raise SystemExit(f"{artifact}: invalid sdist: {exc}") from exc
    return files


def _artifact_files(artifact: Path) -> dict[PurePosixPath, bytes]:
    if artifact.name.endswith(".whl"):
        return _wheel_files(artifact)
    if artifact.name.endswith(".tar.gz"):
        return _sdist_files(artifact)
    raise SystemExit(f"unsupported distribution artifact: {artifact}")


def check(artifact: Path, canonical: dict[PurePosixPath, bytes]) -> None:
    bundled = _artifact_files(artifact)
    missing = canonical.keys() - bundled.keys()
    extra = bundled.keys() - canonical.keys()
    changed = {
        path
        for path in canonical.keys() & bundled.keys()
        if canonical[path] != bundled[path]
    }
    if missing or extra or changed:
        details = []
        for label, paths in (("missing", missing), ("extra", extra), ("changed", changed)):
            if paths:
                details.append(f"{label}: {', '.join(sorted(str(path) for path in paths))}")
        raise SystemExit(
            f"{artifact}: bundled setup skill differs from canonical source "
            f"({'; '.join(details)})"
        )
    print(f"{artifact}: bundled setup skill matches canonical source ({len(canonical)} files)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifacts", nargs="+", type=Path)
    parser.add_argument("--canonical", type=Path, default=DEFAULT_CANONICAL)
    args = parser.parse_args()

    canonical = _canonical_files(args.canonical)
    for artifact in args.artifacts:
        check(artifact, canonical)


if __name__ == "__main__":
    main()
