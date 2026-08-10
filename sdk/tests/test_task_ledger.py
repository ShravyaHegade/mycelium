"""TaskLedger: task-level durable records and idempotency guard.

Sibling test for ``mycelium/task_ledger.py`` — covers the public storage
classes, claim/complete/fail lifecycle, request-id derivation priority, the
sync + async ``@task_ledger`` decorators, and receipt emission.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mycelium import (
    AuditReceiptEmitter,
    TaskFileLedgerStorage,
    TaskInMemoryLedgerStorage,
    TaskLedger,
    TaskLedgerEntry,
    TaskLedgerError,
    TaskLedgerPendingError,
    get_task_ledger,
    task_ledger,
    task_ledger_sync,
)

# ---------------------------------------------------------------------------
# Entry + storage primitives
# ---------------------------------------------------------------------------


def test_task_entry_to_dict_from_dict_roundtrip() -> None:
    entry = TaskLedgerEntry(
        request_id="r1",
        task="process_invoice",
        args=[1, "x"],
        kwargs={"amount": 5.0},
        status="completed",
        result={"ok": True},
        error=None,
        started_at=1.0,
        finished_at=2.0,
    )
    restored = TaskLedgerEntry.from_dict(entry.to_dict())
    assert restored == entry


def test_task_in_memory_storage_get_set_list_all() -> None:
    storage = TaskInMemoryLedgerStorage()
    assert storage.get("missing") is None
    entry = TaskLedgerEntry(request_id="r1", task="t", args=[], kwargs={}, status="in-flight")
    storage.set(entry)
    assert storage.get("r1") == entry
    assert storage.list_all() == [entry]


def test_task_file_storage_persists(tmp_path: Path) -> None:
    path = tmp_path / "tasks.json"
    storage = TaskFileLedgerStorage(path)
    entry = TaskLedgerEntry(request_id="r1", task="t", args=[], kwargs={}, status="in-flight")
    storage.set(entry)
    assert storage.get("r1") == entry
    assert [e.request_id for e in storage.list_all()] == ["r1"]

    reloaded = TaskFileLedgerStorage(path)
    assert reloaded.get("r1") == entry


def test_task_file_storage_claim_completed_and_in_flight(tmp_path: Path) -> None:
    path = tmp_path / "tasks.json"
    storage = TaskFileLedgerStorage(path)

    completed = TaskLedgerEntry(
        request_id="done", task="t", args=[], kwargs={}, status="completed", result=42
    )
    storage.set(completed)
    outcome, existing = storage.try_claim_inflight(completed)
    assert outcome == "completed"
    assert existing == completed

    in_flight = TaskLedgerEntry(
        request_id="busy", task="t", args=[], kwargs={}, status="in-flight"
    )
    storage.set(in_flight)
    outcome, existing = storage.try_claim_inflight(in_flight)
    assert outcome == "in_flight"
    assert existing == in_flight


def test_task_file_storage_claim_overwrites_failed(tmp_path: Path) -> None:
    path = tmp_path / "tasks.json"
    storage = TaskFileLedgerStorage(path)
    failed = TaskLedgerEntry(
        request_id="f1", task="t", args=[], kwargs={}, status="failed", error="boom"
    )
    storage.set(failed)
    fresh = TaskLedgerEntry(request_id="f1", task="t", args=[], kwargs={}, status="in-flight")
    outcome, existing = storage.try_claim_inflight(fresh)
    assert outcome == "claimed"
    assert existing is None
    assert storage.get("f1").status == "in-flight"


# ---------------------------------------------------------------------------
# TaskLedger claim / complete / fail lifecycle
# ---------------------------------------------------------------------------


def test_claim_new_task_returns_in_flight_entry() -> None:
    ledger = TaskLedger()
    entry = ledger.claim("r1", "process_invoice", (1,), {"amount": 5.0})
    assert entry.request_id == "r1"
    assert entry.task == "process_invoice"
    assert entry.status == "in-flight"
    assert entry.args == [1]
    assert entry.kwargs == {"amount": 5.0}


def test_claim_completed_task_returns_cached_entry() -> None:
    ledger = TaskLedger()
    ledger.claim("r1", "process_invoice", (1,), {"amount": 5.0})
    ledger.complete("r1", {"ok": True})

    replayed = ledger.claim("r1", "process_invoice", (1,), {"amount": 5.0})
    assert replayed.status == "completed"
    assert replayed.result == {"ok": True}


def test_claim_in_flight_task_raises_pending() -> None:
    ledger = TaskLedger()
    ledger.claim("r1", "process_invoice", (1,), {"amount": 5.0})
    with pytest.raises(TaskLedgerPendingError, match="already in-flight"):
        ledger.claim("r1", "process_invoice", (1,), {"amount": 5.0})


def test_complete_unknown_request_raises() -> None:
    ledger = TaskLedger()
    with pytest.raises(TaskLedgerError, match="unknown request"):
        ledger.complete("missing", {"ok": True})


def test_fail_unknown_request_raises() -> None:
    ledger = TaskLedger()
    with pytest.raises(TaskLedgerError, match="unknown request"):
        ledger.fail("missing", RuntimeError("boom"))


def test_complete_records_result_and_finished_at() -> None:
    ledger = TaskLedger()
    ledger.claim("r1", "process_invoice", (1,), {})
    completed = ledger.complete("r1", {"ok": True})
    assert completed.status == "completed"
    assert completed.result == {"ok": True}
    assert completed.finished_at is not None
    stored = ledger.get("r1")
    assert stored.status == "completed"
    assert stored.result == {"ok": True}


def test_fail_records_error_and_keeps_failed_status() -> None:
    ledger = TaskLedger()
    ledger.claim("r1", "process_invoice", (1,), {})
    failed = ledger.fail("r1", RuntimeError("gateway down"))
    assert failed.status == "failed"
    assert "RuntimeError" in (failed.error or "")
    stored = ledger.get("r1")
    assert stored.status == "failed"


def test_get_returns_none_for_unknown() -> None:
    assert TaskLedger().get("missing") is None


# ---------------------------------------------------------------------------
# Request-id derivation priority
# ---------------------------------------------------------------------------


def test_derive_request_id_prefers_task_id() -> None:
    ledger = TaskLedger()
    rid = ledger.derive_request_id(
        "process_invoice", (), {"task_id": "t-1", "amount": 5.0}
    )
    assert rid == "t-1"


def test_derive_request_id_uses_run_id_second() -> None:
    ledger = TaskLedger()
    rid = ledger.derive_request_id(
        "process_invoice", (), {"run_id": "run-9", "amount": 5.0}
    )
    assert rid == "process_invoice:run-9"


def test_derive_request_id_uses_id_from_fields() -> None:
    ledger = TaskLedger(id_from=["account_id", "region"])
    rid = ledger.derive_request_id(
        "process_invoice", (), {"account_id": "acct_1", "region": "eu", "amount": 5.0}
    )
    assert rid == "process_invoice:account_id=acct_1:region=eu"


def test_derive_request_id_stable_hash_of_args_and_kwargs() -> None:
    ledger = TaskLedger()
    a = ledger.derive_request_id("process_invoice", (1, 2), {"amount": 5.0})
    b = ledger.derive_request_id("process_invoice", (1, 2), {"amount": 5.0})
    c = ledger.derive_request_id("process_invoice", (1, 2), {"amount": 6.0})
    assert a == b
    assert a != c


def test_derive_request_id_task_id_beats_run_id() -> None:
    ledger = TaskLedger()
    rid = ledger.derive_request_id(
        "process_invoice", (), {"task_id": "t-1", "run_id": "run-9"}
    )
    assert rid == "t-1"


# ---------------------------------------------------------------------------
# Sync + async decorators
# ---------------------------------------------------------------------------


def test_task_ledger_sync_records_completion() -> None:
    calls: list[int] = []

    @task_ledger_sync()
    def process_invoice(invoice_id: int) -> dict:
        calls.append(invoice_id)
        return {"processed": invoice_id}

    result = process_invoice(invoice_id=10)
    assert result == {"processed": 10}
    assert calls == [10]


def test_task_ledger_sync_dedupes_by_task_id() -> None:
    calls: list[int] = []

    @task_ledger_sync()
    def process_invoice(invoice_id: int) -> dict:
        calls.append(invoice_id)
        return {"processed": invoice_id}

    first = process_invoice(invoice_id=10, task_id="dup-1")
    second = process_invoice(invoice_id=10, task_id="dup-1")
    assert first == second == {"processed": 10}
    assert calls == [10]


def test_task_ledger_sync_records_failure() -> None:
    @task_ledger_sync()
    def process_invoice(invoice_id: int) -> dict:
        raise RuntimeError("gateway down")

    with pytest.raises(RuntimeError, match="gateway down"):
        process_invoice(invoice_id=10)


def test_task_ledger_sync_attaches_ledger() -> None:
    @task_ledger_sync()
    def process_invoice(invoice_id: int) -> dict:
        return {"processed": invoice_id}

    assert isinstance(get_task_ledger(process_invoice), TaskLedger)


def test_task_ledger_sync_strips_bookkeeping_kwargs_from_body() -> None:
    seen: dict = {}

    @task_ledger_sync()
    def process_invoice(invoice_id: int, **kwargs) -> dict:
        seen.update(kwargs)
        return {"processed": invoice_id}

    process_invoice(invoice_id=10, task_id="t-1", run_id="r-1", amount=5.0)
    assert seen == {"amount": 5.0}


def test_task_ledger_sync_reruns_failed_task_on_retry() -> None:
    calls: list[int] = []

    @task_ledger_sync()
    def flaky(invoice_id: int) -> dict:
        calls.append(invoice_id)
        if len(calls) == 1:
            raise RuntimeError("gateway down")
        return {"processed": invoice_id}

    with pytest.raises(RuntimeError):
        flaky(invoice_id=1, task_id="flaky-1")
    result = flaky(invoice_id=1, task_id="flaky-1")
    assert result == {"processed": 1}
    assert calls == [1, 1]


async def test_task_ledger_async_records_completion() -> None:
    calls: list[int] = []

    @task_ledger()
    async def process_invoice(invoice_id: int) -> dict:
        await asyncio.sleep(0)
        calls.append(invoice_id)
        return {"processed": invoice_id}

    result = await process_invoice(invoice_id=10)
    assert result == {"processed": 10}
    assert calls == [10]


async def test_task_ledger_async_dedupes_by_task_id() -> None:
    calls: list[int] = []

    @task_ledger()
    async def process_invoice(invoice_id: int) -> dict:
        calls.append(invoice_id)
        return {"processed": invoice_id}

    first = await process_invoice(invoice_id=10, task_id="dup-1")
    second = await process_invoice(invoice_id=10, task_id="dup-1")
    assert first == second == {"processed": 10}
    assert calls == [10]


async def test_task_ledger_async_records_failure() -> None:
    @task_ledger()
    async def process_invoice(invoice_id: int) -> dict:
        raise RuntimeError("gateway down")

    with pytest.raises(RuntimeError, match="gateway down"):
        await process_invoice(invoice_id=10)


async def test_task_ledger_async_attaches_ledger() -> None:
    @task_ledger()
    async def process_invoice(invoice_id: int) -> dict:
        return {"processed": invoice_id}

    assert isinstance(get_task_ledger(process_invoice), TaskLedger)


def test_file_storage_dedupe_across_two_ledgers(tmp_path: Path) -> None:
    path = tmp_path / "tasks.json"

    @task_ledger_sync(storage=TaskFileLedgerStorage(path))
    def first(invoice_id: int) -> dict:
        return {"processed": invoice_id}

    first(invoice_id=10, task_id="shared-1")

    @task_ledger_sync(storage=TaskFileLedgerStorage(path))
    def second(invoice_id: int) -> dict:
        raise AssertionError("must not re-run")

    result = second(invoice_id=10, task_id="shared-1")
    assert result == {"processed": 10}


# ---------------------------------------------------------------------------
# Receipt emission
# ---------------------------------------------------------------------------


def test_task_ledger_sync_emits_receipt_on_success_and_failure() -> None:
    emitter = AuditReceiptEmitter(agent_id="agent_a", signing_key="test-key")

    @task_ledger_sync(audit_emitter=emitter)
    def process_invoice(invoice_id: int) -> dict:
        if invoice_id < 0:
            raise RuntimeError("bad invoice")
        return {"processed": invoice_id}

    process_invoice(invoice_id=10, task_id="rec-1")
    with pytest.raises(RuntimeError):
        process_invoice(invoice_id=-1, task_id="rec-2")

    receipts = emitter.storage.list_all()
    assert [r.request_id for r in receipts] == ["rec-1", "rec-2"]
    assert receipts[0].status == "completed"
    assert receipts[1].status == "failed"
    assert receipts[0].action_kind == "task"


def test_task_ledger_sync_without_emitter_no_receipt() -> None:
    @task_ledger_sync()
    def process_invoice(invoice_id: int) -> dict:
        return {"processed": invoice_id}

    process_invoice(invoice_id=10)
