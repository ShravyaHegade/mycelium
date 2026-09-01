"""Durability helpers for LockedJsonDictFile."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from mycelium.storage.json_file import LockedJsonDictFile, StorageCorruptionError


def test_save_fsyncs_tmp_before_replace(tmp_path: Path) -> None:
    path = tmp_path / "ledger.json"
    store = LockedJsonDictFile(path)
    fsync_fds: list[int] = []

    real_fsync = __import__("os").fsync

    def _track_fsync(fd: int) -> None:
        fsync_fds.append(fd)
        real_fsync(fd)

    with patch("mycelium.storage.json_file.os.fsync", side_effect=_track_fsync):
        store.save({"k1": {"v": 1}})

    assert path.exists()
    assert store.load() == {"k1": {"v": 1}}
    # tmp file fsync + best-effort directory fsync
    assert len(fsync_fds) >= 1


@pytest.mark.parametrize("payload", ['{"old-run":', "[]"])
def test_load_rejects_corrupt_json_without_rewriting(
    tmp_path: Path,
    payload: str,
) -> None:
    path = tmp_path / "state.json"
    path.write_text(payload, encoding="utf-8")
    original = path.read_bytes()

    with pytest.raises(StorageCorruptionError):
        LockedJsonDictFile(path).load()

    assert path.read_bytes() == original


@pytest.mark.parametrize("payload", ['{"old-run":', "[]"])
def test_read_modify_write_rejects_corrupt_json_without_overwriting(
    tmp_path: Path,
    payload: str,
) -> None:
    path = tmp_path / "state.json"
    path.write_text(payload, encoding="utf-8")
    original = path.read_bytes()

    with pytest.raises(StorageCorruptionError):
        LockedJsonDictFile(path).read_modify_write(
            lambda data: data.update({"new-run": {"steps": 1}})
        )

    assert path.read_bytes() == original
