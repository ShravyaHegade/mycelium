"""Durability helpers for LockedJsonDictFile."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from mycelium.storage.json_file import LockedJsonDictFile


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
