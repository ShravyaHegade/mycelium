"""Process-kill / crash-window tests (Phase 4 / process-kill + crash-window).

``test_side_effect_resolution.py`` already simulates a crash-after-claim by
planting an expired ``LedgerEntry`` in-process. These tests go further: a real
child OS process claims a payment transition, records a provider handle, and is
SIGKILLed mid-effect before it can ``complete()``. The parent then redispatches
the same transition key against a mock Reconciler:

- ``COMPLETED``    -> redispatch returns the reconciled receipt; executions == 1
- ``NOT_EXECUTED`` -> redispatch runs the tool exactly once more; executions == 2
- ``UNKNOWN``      -> HARD_BLOCK; executions stay 1
- crash before any ``external_operation_ref`` is recorded -> fail-closed
  HARD_BLOCK (no provider lookup); executions stay 1

Determinism: short lease TTLs, lease auto-renew disabled on the child so the
lease lapses after the kill, bounded waits for the ready marker, and an atomic
cross-process execution counter file.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import time
import uuid
from pathlib import Path
from typing import Any

from mycelium import (
    FileLedgerStorage,
    LedgerHardBlockError,
    ReconcileResult,
    ReconcileStatus,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
    record_external_operation,
    side_effect,
)

_MP_CTX = mp.get_context("spawn")

_LEASE_TTL = 1.0  # short: lease lapses quickly after the child is killed
_READY_TIMEOUT = 10.0


def _append_count(path: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, b"x\n")
    finally:
        os.close(fd)


def _append_line(path: str, line: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line.encode("utf-8") + b"\n")
    finally:
        os.close(fd)


def _payment_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="crash",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _crash_worker(payload: dict[str, Any]) -> None:
    """Claim a payment transition, (optionally) record the provider handle,
    signal ready, then run a long effect and never complete.

    The parent SIGKILLs this worker mid-effect, leaving an ambiguous
    ``IN_FLIGHT`` / ``maybe_crossed`` entry whose lease later expires.
    """
    from mycelium import FileLedgerStorage, ledger_sync

    storage = FileLedgerStorage(payload["ledger_path"])

    @ledger_sync(
        storage=storage,
        transition_binding=_payment_binding(),
        lease_ttl=float(payload["lease_ttl"]),
        lease_renew_interval=0,  # no heartbeat renewal: lease lapses after kill
        poll_interval=0.02,
        poll_timeout=float(payload["poll_timeout"]),
    )
    def charge(amount: float) -> dict[str, Any]:
        _append_count(payload["exec_file"])
        with side_effect():
            if payload.get("record_ref"):
                record_external_operation(payload["op_ref"])
            _append_line(payload["ready_file"], "ready")
            time.sleep(60.0)
        return {"charged": True}

    try:
        with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
            charge(amount=10.0, tool_call_id=payload["tool_call_id"])
    except Exception as exc:  # noqa: BLE001 — surface to parent
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")


class _ScriptedReconciler:
    """Mock Reconciler returning a scripted result and recording calls."""

    def __init__(self, status: ReconcileStatus, result: Any = None) -> None:
        self._status = status
        self._result = result
        self.calls: list[str] = []

    def reconcile(self, entry: Any) -> ReconcileResult:
        self.calls.append(entry.request_id)
        if self._status == ReconcileStatus.COMPLETED:
            return ReconcileResult.completed(self._result)
        if self._status == ReconcileStatus.NOT_EXECUTED:
            return ReconcileResult.not_executed()
        return ReconcileResult.unknown()


def _crash_then_redispatch(
    tmp_path: Path,
    reconciler: _ScriptedReconciler,
    *,
    record_ref: bool,
) -> tuple[Path, int, Any, _ScriptedReconciler]:
    """Kill a mid-effect worker, wait for the lease to lapse, redispatch.

    Returns ``(exec_file, executions, outcome, reconciler)`` where ``outcome``
    is the redispatch result dict or ``LedgerHardBlockError``.
    """
    ledger_path = tmp_path / "ledger.json"
    exec_file = tmp_path / "executions.txt"
    ready_file = tmp_path / "ready.txt"
    err_file = tmp_path / "err.txt"
    tool_call_id = f"call_crash_{uuid.uuid4().hex}"

    payload = {
        "ledger_path": str(ledger_path),
        "exec_file": str(exec_file),
        "ready_file": str(ready_file),
        "err_file": str(err_file),
        "tool_call_id": tool_call_id,
        "op_ref": "pi_crash_1",
        "record_ref": record_ref,
        "lease_ttl": _LEASE_TTL,
        "poll_timeout": 5.0,
    }

    proc = _MP_CTX.Process(target=_crash_worker, args=(payload,), name="crash-worker")
    proc.start()
    try:
        deadline = time.time() + _READY_TIMEOUT
        while not ready_file.exists():
            if time.time() >= deadline:
                raise AssertionError("crash worker never signaled ready")
            time.sleep(0.02)

        proc.kill()  # SIGKILL: entry stays IN_FLIGHT, never completed
        proc.join(timeout=5.0)
        assert proc.exitcode is not None and proc.exitcode != 0, (
            "crash worker should have been killed"
        )

        # Let the (short) lease lapse so the redispatch sees EXPIRED.
        time.sleep(_LEASE_TTL + 0.4)
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=2.0)

    storage = FileLedgerStorage(ledger_path)

    @ledger_sync(
        storage=storage,
        transition_binding=_payment_binding(),
        reconciler=reconciler,
        lease_ttl=30.0,
        poll_interval=0.02,
        poll_timeout=5.0,
    )
    # NB: same tool name as the killed child so the derived request_id
    # (tool_name + args + kwargs) collides and the redispatch re-claims it.
    def charge(amount: float) -> dict[str, Any]:
        _append_count(str(exec_file))
        return {"charged": True, "redispatch": True}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        try:
            outcome: Any = charge(amount=10.0, tool_call_id=tool_call_id)
        except LedgerHardBlockError:
            outcome = LedgerHardBlockError

    return exec_file, len(exec_file.read_text(encoding="utf-8").splitlines()), outcome, reconciler


def test_kill_before_complete_reconcile_completed_returns_receipt(
    tmp_path: Path,
) -> None:
    reconciler = _ScriptedReconciler(
        ReconcileStatus.COMPLETED, {"charged": True, "id": "pi_crash_1"}
    )
    exec_file, executions, outcome, reconciler = _crash_then_redispatch(
        tmp_path, reconciler, record_ref=True
    )

    assert outcome == {"charged": True, "id": "pi_crash_1"}, outcome
    assert executions == 1, "redispatch must not re-execute after reconcile COMPLETED"
    assert reconciler.calls, "reconciler should have been consulted"

    storage = FileLedgerStorage(tmp_path / "ledger.json")
    entry = storage.get(reconciler.calls[0])
    assert entry is not None
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED
    assert entry.result == {"charged": True, "id": "pi_crash_1"}


def test_kill_before_complete_reconcile_not_executed_runs_once_more(
    tmp_path: Path,
) -> None:
    reconciler = _ScriptedReconciler(ReconcileStatus.NOT_EXECUTED)
    exec_file, executions, outcome, reconciler = _crash_then_redispatch(
        tmp_path, reconciler, record_ref=True
    )

    assert outcome == {"charged": True, "redispatch": True}
    assert executions == 2, (
        "reconcile NOT_EXECUTED grants exactly one more execution "
        f"(killed worker + redispatch); got {executions}"
    )
    assert reconciler.calls

    storage = FileLedgerStorage(tmp_path / "ledger.json")
    entry = storage.get(reconciler.calls[0])
    assert entry is not None
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED
    assert entry.result == {"charged": True, "redispatch": True}


def test_kill_before_complete_reconcile_unknown_hard_blocks(tmp_path: Path) -> None:
    reconciler = _ScriptedReconciler(ReconcileStatus.UNKNOWN)
    exec_file, executions, outcome, reconciler = _crash_then_redispatch(
        tmp_path, reconciler, record_ref=True
    )

    assert outcome is LedgerHardBlockError
    assert executions == 1, "UNKNOWN reconcile must not re-execute"
    assert reconciler.calls

    storage = FileLedgerStorage(tmp_path / "ledger.json")
    entry = storage.get(reconciler.calls[0])
    assert entry is not None
    assert entry.resolved_terminal_outcome() in (
        TerminalOutcome.BLOCKED,
        TerminalOutcome.UNKNOWN,
    )


def test_kill_before_ref_recorded_hard_blocks_no_provider_lookup(
    tmp_path: Path,
) -> None:
    """Crash before any provider ref was recorded: fail-closed hard-block; the
    Reconciler must never be consulted (no ref -> no lookup)."""
    reconciler = _ScriptedReconciler(ReconcileStatus.COMPLETED, {"nope": True})
    exec_file, executions, outcome, reconciler = _crash_then_redispatch(
        tmp_path, reconciler, record_ref=False
    )

    assert outcome is LedgerHardBlockError
    assert executions == 1, "no ref recorded -> no re-execution"
    assert reconciler.calls == [], (
        "reconciler must not be consulted without an external_operation_ref"
    )

    storage = FileLedgerStorage(tmp_path / "ledger.json")
    entries = storage.list_all()
    assert len(entries) == 1
    assert entries[0].external_operation_ref is None
    assert entries[0].resolved_terminal_outcome() in (
        TerminalOutcome.BLOCKED,
        TerminalOutcome.UNKNOWN,
    )
