"""Pagination, export, and retention lifecycle checks."""

from __future__ import annotations

import json
import time
from dataclasses import replace

import pytest

from mycelium import ActionLedger, FileLedgerStorage, LedgerEntry, TerminalOutcome
from mycelium.__main__ import main
from mycelium.storage.redis_ledger import RedisLedgerStorage


def _terminal(request_id: str, outcome: TerminalOutcome, *, started_at: float) -> LedgerEntry:
    status = "completed" if outcome == TerminalOutcome.COMPLETED else "failed"
    return LedgerEntry(
        request_id=request_id,
        tool="charge",
        args=[],
        kwargs={},
        status=status,
        terminal_outcome=outcome.value,
        started_at=started_at,
        finished_at=started_at + 1,
    )


def test_transition_page_cursor_and_filters() -> None:
    from mycelium import InMemoryLedgerStorage

    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage)
    for index in range(4):
        storage.set(_terminal(f"req-{index}", TerminalOutcome.COMPLETED, started_at=index))
    storage.set(_terminal("blocked", TerminalOutcome.BLOCKED, started_at=5))

    first = ledger.list_transitions_page(
        limit=2,
        outcome=TerminalOutcome.COMPLETED,
    )
    second = ledger.list_transitions_page(
        limit=2,
        cursor=first.next_cursor,
        outcome=TerminalOutcome.COMPLETED,
    )

    assert [entry.request_id for entry in first.entries] == ["req-0", "req-1"]
    assert [entry.request_id for entry in second.entries] == ["req-2", "req-3"]
    assert second.next_cursor is None


def test_redis_uses_indexes_for_filtered_pages_and_safe_prune(monkeypatch) -> None:
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("redis.Redis.from_url", lambda *args, **kwargs: fake)
    storage = RedisLedgerStorage("redis://test", prefix="retention:")
    ledger = ActionLedger(storage=storage)
    old = time.time() - 10_000
    storage.set(_terminal("done", TerminalOutcome.COMPLETED, started_at=old))
    storage.set(_terminal("blocked", TerminalOutcome.BLOCKED, started_at=old))

    page = ledger.list_transitions_page(limit=10, outcome=TerminalOutcome.COMPLETED)
    assert [entry.request_id for entry in page.entries] == ["done"]
    monkeypatch.setattr(fake, "scan_iter", lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("indexed listing must not scan the keyspace")
    ))
    again = ledger.list_transitions_page(limit=10, outcome=TerminalOutcome.COMPLETED)
    assert [entry.request_id for entry in again.entries] == ["done"]

    candidates, deleted = ledger.prune_transitions(before=time.time(), dry_run=True)
    assert [entry.request_id for entry in candidates] == ["done"]
    assert deleted == 0
    _, deleted = ledger.prune_transitions(before=time.time(), dry_run=False)
    assert deleted == 1
    assert storage.get("done") is None
    assert storage.get("blocked") is not None


def test_cli_export_and_prune_dry_run(tmp_path, capsys) -> None:
    ledger_path = tmp_path / "ledger.json"
    export_path = tmp_path / "archive.ndjson"
    storage = FileLedgerStorage(ledger_path)
    storage.set(
        replace(
            _terminal("done", TerminalOutcome.COMPLETED, started_at=time.time() - 100),
            finished_at=time.time() - 90,
        )
    )

    assert (
        main(
            [
                "transitions",
                "export",
                "--file",
                str(ledger_path),
                "--output",
                str(export_path),
            ]
        )
        == 0
    )
    assert json.loads(export_path.read_text().splitlines()[0])["request_id"] == "done"
    assert main(
        [
            "transitions",
            "prune",
            "--file",
            str(ledger_path),
            "--older-than",
            "1s",
            "--dry-run",
        ]
    ) == 0
    assert "would prune 1 transitions" in capsys.readouterr().out
    assert storage.get("done") is not None
