"""Atomicity contract tests for one-shot terminal outcome transitions.

Every terminal-outcome write (``complete``, ``fail``, ``mark_blocked``,
``mark_unknown``) uses compare-and-swap (CAS) at the storage layer.
These tests verify that:
  1. The legal transition matrix is enforced (no silent overwrites).
  2. Stale workers cannot resolve an already-resolved transition.
  3. Owner mismatch is refused on wrapper-path writes.
  4. Concurrent writers converge to at most one terminal outcome (no lost
     updates), including Redis's WATCH/MULTI path.

See ``sdk/README.md`` § "Atomicity contract" for the full design.
"""

from __future__ import annotations

import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerError,
    LedgerOutcomeAlreadySetError,
    ReconcileResult,
    RedisLedgerStorage,
    SideEffectBoundary,
    SideEffectClass,
    SqliteLedgerStorage,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BINDING = ToolTransitionBinding.for_tool(
    agent_id="test",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)

_SCOPE = TransitionScope(thread_id="t", run_id="r")


def _make_entry(
    request_id: str = "test-req",
    terminal_outcome: str | None = TerminalOutcome.IN_FLIGHT.value,
    owner: str | None = None,
) -> LedgerEntry:
    return LedgerEntry(
        request_id=request_id,
        tool="test_tool",
        args=(),
        kwargs={},
        status=terminal_outcome if terminal_outcome else "in-flight",
        terminal_outcome=terminal_outcome,
        idempotency_key=request_id,
        owner=owner,
    )


def _claim(ledger: ActionLedger, request_id: str) -> LedgerEntry:
    return ledger.claim(request_id, "test_tool", (), {})


def _scope():
    from mycelium.transition import execution_scope

    return execution_scope(_SCOPE)


def _replace_fence(entry: LedgerEntry, fence: int) -> LedgerEntry:
    from dataclasses import replace

    return replace(entry, fence=fence)


def _replace_lease(entry: LedgerEntry, lease_until: float) -> LedgerEntry:
    from dataclasses import replace

    return replace(entry, lease_until=lease_until)


def _set_entry_on_storage(ledger: ActionLedger, request_id: str, entry: LedgerEntry) -> None:
    """Directly write an entry bypassing CAS (for test setup)."""
    ledger.get  # ensure ledger is alive
    from mycelium.action_ledger import InMemoryLedgerStorage as _Mem

    storage = ledger._storage
    if isinstance(storage, _Mem):
        storage._entries[request_id] = entry
    else:
        storage.set(entry)


# ---------------------------------------------------------------------------
# Storage backend fixtures
# ---------------------------------------------------------------------------


def _fake_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)

    def from_url(url: str, **kwargs: object) -> object:
        return fake

    import redis

    monkeypatch.setattr(redis.Redis, "from_url", from_url)


@pytest.fixture(
    params=[
        pytest.param("memory", id="memory"),
        pytest.param("file", id="file"),
        pytest.param("sqlite", id="sqlite"),
        pytest.param("redis", id="redis"),
    ]
)
def ledger(request, tmp_path, monkeypatch):
    if request.param == "memory":
        storage = InMemoryLedgerStorage()
    elif request.param == "file":
        storage = FileLedgerStorage(tmp_path / "ledger.json")
    elif request.param == "sqlite":
        storage = SqliteLedgerStorage(tmp_path / "ledger.db")
    elif request.param == "redis":
        _fake_redis(monkeypatch)
        storage = RedisLedgerStorage("redis://test")
    return ActionLedger(storage=storage)


# ---------------------------------------------------------------------------
# Legal transition matrix
# ---------------------------------------------------------------------------

_TRANSITIONS: list[tuple[str, str, bool]] = [
    # (from_outcome, action, expect_success)
    # --- IN_FLIGHT → any terminal outcome: always allowed ---
    ("IN_FLIGHT", "complete", True),
    ("IN_FLIGHT", "fail_before", True),
    ("IN_FLIGHT", "fail_after", True),
    ("IN_FLIGHT", "mark_blocked", True),
    ("IN_FLIGHT", "mark_unknown", True),
    # --- COMPLETED → nothing allowed ---
    ("COMPLETED", "complete", False),
    ("COMPLETED", "fail_before", False),
    ("COMPLETED", "fail_after", False),
    ("COMPLETED", "mark_blocked", False),
    ("COMPLETED", "mark_unknown", False),
    # --- BLOCKED → nothing allowed via public mutators ---
    ("BLOCKED", "complete", False),
    ("BLOCKED", "fail_before", False),
    ("BLOCKED", "fail_after", False),
    ("BLOCKED", "mark_blocked", False),
    ("BLOCKED", "mark_unknown", False),
    # --- UNKNOWN → nothing allowed via public mutators ---
    ("UNKNOWN", "complete", False),
    ("UNKNOWN", "fail_before", False),
    ("UNKNOWN", "fail_after", False),
    ("UNKNOWN", "mark_blocked", False),
    ("UNKNOWN", "mark_unknown", False),
    # --- FAILED_BEFORE_EFFECT → nothing allowed via public mutators ---
    ("FAILED_BEFORE_EFFECT", "complete", False),
    ("FAILED_BEFORE_EFFECT", "fail_before", False),
    ("FAILED_BEFORE_EFFECT", "fail_after", False),
    ("FAILED_BEFORE_EFFECT", "mark_blocked", False),
    ("FAILED_BEFORE_EFFECT", "mark_unknown", False),
    # --- FAILED_AFTER_EFFECT → nothing allowed via public mutators ---
    ("FAILED_AFTER_EFFECT", "complete", False),
    ("FAILED_AFTER_EFFECT", "fail_before", False),
    ("FAILED_AFTER_EFFECT", "fail_after", False),
    ("FAILED_AFTER_EFFECT", "mark_blocked", False),
    ("FAILED_AFTER_EFFECT", "mark_unknown", False),
]


@pytest.mark.parametrize("from_outcome,action,expect_success", _TRANSITIONS)
def test_transition_matrix(
    ledger: ActionLedger,
    from_outcome: str,
    action: str,
    expect_success: bool,
) -> None:
    request_id = f"matrix-{from_outcome}-{action}"
    entry = _make_entry(
        request_id=request_id,
        terminal_outcome=from_outcome,
    )
    _set_entry_on_storage(ledger, request_id, entry)

    if expect_success:
        if action == "complete":
            ledger.complete(request_id, {"ok": True})
        elif action == "fail_before":
            ledger.fail(request_id, ValueError("x"))
        elif action == "fail_after":
            ledger.fail(request_id, ValueError("x"), failed_after_effect=True)
        elif action == "mark_blocked":
            ledger.mark_blocked(request_id, error="blocked")
        elif action == "mark_unknown":
            ledger.mark_unknown(request_id, error="unknown")
        stored = ledger.get(request_id)
        assert stored is not None
        if action == "complete":
            assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value
        elif action == "fail_before":
            assert stored.terminal_outcome == TerminalOutcome.FAILED_BEFORE_EFFECT.value
        elif action == "fail_after":
            assert stored.terminal_outcome == TerminalOutcome.FAILED_AFTER_EFFECT.value
        elif action == "mark_blocked":
            assert stored.terminal_outcome == TerminalOutcome.BLOCKED.value
        elif action == "mark_unknown":
            assert stored.terminal_outcome == TerminalOutcome.UNKNOWN.value
    else:
        with pytest.raises(LedgerOutcomeAlreadySetError):
            if action == "complete":
                ledger.complete(request_id, {"ok": True})
            elif action == "fail_before":
                ledger.fail(request_id, ValueError("x"))
            elif action == "fail_after":
                ledger.fail(request_id, ValueError("x"), failed_after_effect=True)
            elif action == "mark_blocked":
                ledger.mark_blocked(request_id, error="blocked")
            elif action == "mark_unknown":
                ledger.mark_unknown(request_id, error="unknown")


# ---------------------------------------------------------------------------
# Resolution paths (release, reconcile) accept the broader set of outcomes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("from_outcome", ["BLOCKED", "UNKNOWN", "FAILED_AFTER_EFFECT"])
def test_release_completed_from_terminal_outcomes(ledger: ActionLedger, from_outcome: str) -> None:
    request_id = f"release-{from_outcome}"
    entry = _make_entry(
        request_id=request_id,
        terminal_outcome=from_outcome,
    )
    _set_entry_on_storage(ledger, request_id, entry)
    released = ledger.release(
        request_id,
        verified="completed",
        result={"released": True},
        by="ops",
        reason="verified externally",
    )
    assert released.terminal_outcome == TerminalOutcome.COMPLETED.value
    assert released.result == {"released": True}


def test_release_not_executed_from_blocked(ledger: ActionLedger) -> None:
    request_id = "release-not-exec"
    entry = _make_entry(
        request_id=request_id,
        terminal_outcome=TerminalOutcome.BLOCKED.value,
    )
    _set_entry_on_storage(ledger, request_id, entry)
    released = ledger.release(
        request_id,
        verified="not_executed",
        by="ops",
        reason="provider confirms no effect",
    )
    assert released.operator_resolution == "not_executed"
    # Entry remains BLOCKED on the terminal outcome (resolution is separate)
    assert released.terminal_outcome == TerminalOutcome.BLOCKED.value


# ---------------------------------------------------------------------------
# Stalled-worker scenario
# ---------------------------------------------------------------------------


def test_stalled_worker_cannot_overwrite_completed(ledger: ActionLedger) -> None:
    """A stale worker that wakes after the transition was resolved by an
    operator or a retry must not silently overwrite the true outcome."""
    request_id = "stalled-worker"
    _claim(ledger, request_id)

    ledger.complete(request_id, {"real": "result"})

    # Stale worker tries to fail
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.fail(request_id, RuntimeError("stale"))

    stored = ledger.get(request_id)
    assert stored is not None
    assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value
    assert stored.result == {"real": "result"}


def test_stalled_worker_cannot_overwrite_failed(ledger: ActionLedger) -> None:
    """Same as above but the entry was already failed by another worker."""
    request_id = "stalled-worker-fail"
    _claim(ledger, request_id)

    ledger.fail(request_id, ValueError("real failure"))

    # Stale worker tries to complete
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(request_id, {"fake": "result"})

    stored = ledger.get(request_id)
    assert stored is not None
    assert stored.terminal_outcome == TerminalOutcome.FAILED_BEFORE_EFFECT.value


# ---------------------------------------------------------------------------
# Owner fencing
# ---------------------------------------------------------------------------


def test_owner_mismatch_on_complete(ledger: ActionLedger) -> None:
    """When owner fencing is enabled, a different owner's write is refused."""
    request_id = "owner-fence"
    entry = _make_entry(request_id=request_id, owner="worker-A")
    _set_entry_on_storage(ledger, request_id, entry)

    # worker-B cannot complete
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(request_id, {"ok": True}, _expected_owner="worker-B")


def test_owner_match_succeeds(ledger: ActionLedger) -> None:
    """Same owner can complete."""
    request_id = "owner-match"
    entry = _make_entry(request_id=request_id, owner="worker-A")
    _set_entry_on_storage(ledger, request_id, entry)

    ledger.complete(request_id, {"ok": True}, _expected_owner="worker-A")
    stored = ledger.get(request_id)
    assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value


# ---------------------------------------------------------------------------
# Two-thread race: concurrent writers must converge to at most one outcome
# ---------------------------------------------------------------------------


def test_concurrent_complete_race(ledger: ActionLedger) -> None:
    """Two threads racing to complete the same in-flight entry must not both
    succeed — at most one writer wins, the other gets a refusal.

    With the old ``RedisEntryStorage.try_transition`` (bare ``client.watch()``
    + separate pipeline without ``WatchError`` retry), this test would
    reveal lost updates because the WATCH on the client connection was a
    no-op and the pipeline operated on a separate connection.  The fix uses
    ``pipe.watch()`` inside the pipeline context with a ``WatchError`` retry
    loop, making the CAS genuinely atomic.

    The race window is widened by a ``threading.Barrier`` so both threads
    reach the check phase before either writes.
    """
    request_id = "race-complete"
    _claim(ledger, request_id)

    results: list[Exception | str] = []
    barrier = threading.Barrier(2, timeout=5)

    def _racer(result_value: str) -> None:
        barrier.wait()
        try:
            ledger.complete(request_id, {"result": result_value})
            results.append(result_value)
        except LedgerOutcomeAlreadySetError as e:
            results.append(e)

    t1 = threading.Thread(target=_racer, args=("A",), daemon=True)
    t2 = threading.Thread(target=_racer, args=("B",), daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert len(results) == 2
    successes = [r for r in results if isinstance(r, str)]
    errors = [r for r in results if isinstance(r, LedgerOutcomeAlreadySetError)]
    assert len(successes) == 1, (
        f"expected exactly one successful write, got {len(successes)}: {successes}"
    )
    assert len(errors) == 1, f"expected exactly one refusal, got {len(errors)}"
    stored = ledger.get(request_id)
    assert stored is not None
    assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value


def test_concurrent_complete_and_fail_race(ledger: ActionLedger) -> None:
    """Thread 1 completes, thread 2 fails — only one should win."""
    request_id = "race-complete-fail"
    _claim(ledger, request_id)

    success: list[str] = []
    barrier = threading.Barrier(2, timeout=5)

    def _completer() -> None:
        barrier.wait()
        try:
            ledger.complete(request_id, {"ok": True})
            success.append("complete")
        except LedgerOutcomeAlreadySetError:
            pass

    def _failer() -> None:
        barrier.wait()
        try:
            ledger.fail(request_id, RuntimeError("fail"))
            success.append("fail")
        except LedgerOutcomeAlreadySetError:
            pass

    t1 = threading.Thread(target=_completer, daemon=True)
    t2 = threading.Thread(target=_failer, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert len(success) == 1, (
        f"expected exactly one successful write, got {len(success)}: {success}"
    )
    stored = ledger.get(request_id)
    assert stored is not None
    if success[0] == "complete":
        assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value
    else:
        assert stored.terminal_outcome in (
            TerminalOutcome.FAILED_BEFORE_EFFECT.value,
            TerminalOutcome.FAILED_AFTER_EFFECT.value,
        )


# ---------------------------------------------------------------------------
# Wrapper-path owner fencing (via _run_ledgered)
# ---------------------------------------------------------------------------


def test_wrapper_owner_fencing_prevents_stale_overwrite(ledger: ActionLedger) -> None:
    """The @ledger_sync / @ledger wrapper passes _ledger_owner() as
    _expected_owner, so a different process's write is refused."""
    request_id = "wrapper-owner-fence"
    entry = _make_entry(request_id=request_id, owner="alice")
    _set_entry_on_storage(ledger, request_id, entry)

    # alice's owner matches — allowed
    ledger.complete(request_id, {"ok": True}, _expected_owner="alice")
    stored = ledger.get(request_id)
    assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value

    # Set up a fresh entry again for the negative test
    request_id2 = "wrapper-owner-fence-2"
    entry2 = _make_entry(request_id=request_id2, owner="alice")
    _set_entry_on_storage(ledger, request_id2, entry2)

    # bob (wrong owner) — refused
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(request_id2, {"ok": True}, _expected_owner="bob")
    stored2 = ledger.get(request_id2)
    assert stored2.terminal_outcome == TerminalOutcome.IN_FLIGHT.value


# ---------------------------------------------------------------------------
# Reclaim race: two threads must not both get "claimed" from try_claim_inflight
# ---------------------------------------------------------------------------


def test_concurrent_reclaim_race_inmemory() -> None:
    """In-memory reclaim race: at most one thread gets ``claimed``."""
    storage = InMemoryLedgerStorage()
    request_id = "race-reclaim-mem"
    now = time.time()

    expired = LedgerEntry(
        request_id=request_id,
        tool="t",
        args=(),
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        lease_until=now - 10,
        idempotency_key=request_id,
    )
    storage.set(expired)

    fresh = LedgerEntry(
        request_id=request_id,
        tool="t",
        args=(),
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        lease_until=now + 3600,
        idempotency_key=request_id,
    )

    results: list[tuple[str, LedgerEntry | None]] = []
    barrier = threading.Barrier(2, timeout=5)

    def _racer() -> None:
        barrier.wait()
        outcome, existing = storage.try_claim_inflight(fresh, lease_ttl=3600.0)
        results.append((outcome, existing))

    t1 = threading.Thread(target=_racer, daemon=True)
    t2 = threading.Thread(target=_racer, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    outcomes = [r[0] for r in results]
    claimed = [o for o in outcomes if o == "claimed"]
    assert len(claimed) == 1, f"expected exactly one 'claimed', got {len(claimed)}: {results}"


def test_concurrent_reclaim_race_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    """Redis reclaim race: at most one thread gets ``claimed``."""
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)

    def from_url(url: str, **kwargs: object) -> object:
        return fake

    import redis as redis_mod

    monkeypatch.setattr(redis_mod.Redis, "from_url", from_url)

    storage = RedisLedgerStorage("redis://test")
    request_id = "race-reclaim-redis"
    now = time.time()

    expired = LedgerEntry(
        request_id=request_id,
        tool="t",
        args=(),
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        lease_until=now - 10,
        idempotency_key=request_id,
    )
    storage.set(expired)

    fresh = LedgerEntry(
        request_id=request_id,
        tool="t",
        args=(),
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        lease_until=now + 3600,
        idempotency_key=request_id,
    )

    results: list[tuple[str, LedgerEntry | None]] = []
    barrier = threading.Barrier(2, timeout=5)

    def _racer() -> None:
        barrier.wait()
        outcome, existing = storage.try_claim_inflight(fresh, lease_ttl=3600.0)
        results.append((outcome, existing))

    t1 = threading.Thread(target=_racer, daemon=True)
    t2 = threading.Thread(target=_racer, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    outcomes = [r[0] for r in results]
    claimed = [o for o in outcomes if o == "claimed"]
    assert len(claimed) == 1, f"expected exactly one 'claimed', got {len(claimed)}: {results}"


# ---------------------------------------------------------------------------
# Reconcile NOT_EXECUTED race: CAS loser polls/returns instead of hard-block
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", ["memory", "file", "sqlite", "redis"])
def test_concurrent_reconcile_not_executed_race(
    backend: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two threads racing on reconcile NOT_EXECUTED: the CAS loser re-reads
    and returns the winner's entry instead of hard-blocking. Both threads
    see the same completed result and the tool runs exactly once."""
    if backend == "memory":
        storage = InMemoryLedgerStorage()
    elif backend == "file":
        storage = FileLedgerStorage(tmp_path / "reconcile_race.json")
    elif backend == "sqlite":
        storage = SqliteLedgerStorage(tmp_path / "reconcile_race.db")
    elif backend == "redis":
        fakeredis = pytest.importorskip("fakeredis")
        fake = fakeredis.FakeRedis(decode_responses=True)
        import redis as redis_mod

        monkeypatch.setattr(redis_mod.Redis, "from_url", lambda url, **kw: fake)
        storage = RedisLedgerStorage("redis://test")

    class NotExecutedReconciler:
        def __init__(self) -> None:
            self.entries: list[str] = []

        def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
            self.entries.append(entry.request_id)
            return ReconcileResult.not_executed()

    ledger_inst = ActionLedger(
        storage=storage,
        reconciler=NotExecutedReconciler(),
        poll_interval=0.01,
        poll_timeout=2.0,
    )
    binding = _BINDING
    request_id = "race-reconcile-not-exec"

    entry = _make_entry(
        request_id=request_id,
        terminal_outcome=TerminalOutcome.BLOCKED.value,
    )
    _set_entry_on_storage(ledger_inst, request_id, entry)
    stored = ledger_inst.get(request_id)
    from dataclasses import replace

    ledger_inst._set_entry(replace(stored, external_operation_ref="pi_race"))

    exec_count = 0
    results: list[tuple[str, Any]] = []
    barrier = threading.Barrier(2, timeout=5)

    def _racer() -> None:
        nonlocal exec_count
        barrier.wait()
        entry = ledger_inst.claim_side_effecting(
            request_id,
            "test_tool",
            (),
            {},
            binding,
        )
        if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
            results.append(("completed", entry.result))
        elif entry.terminal_outcome == TerminalOutcome.IN_FLIGHT.value:
            exec_count += 1
            result_value = {"ran": True}
            ledger_inst.complete(request_id, result_value)
            results.append(("ran_tool", result_value))
        else:
            results.append((entry.terminal_outcome or "unknown", None))

    t1 = threading.Thread(target=_racer, daemon=True)
    t2 = threading.Thread(target=_racer, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert exec_count == 1, f"expected exactly 1 tool execution, got {exec_count}"
    statuses = [r[0] for r in results]
    assert "ran_tool" in statuses, f"nobody ran the tool: {results}"
    assert "completed" in statuses, f"nobody saw the completed result: {results}"
    result_values = [r[1] for r in results]
    assert result_values[0] == result_values[1], (
        f"both threads should see the same result: {results}"
    )


@pytest.mark.parametrize("backend", ["memory", "file", "sqlite", "redis"])
def test_concurrent_reconcile_not_executed_race_expired_seed(
    backend: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same race as above but seeded from EXPIRED (lease-expired IN_FLIGHT
    with not_crossed boundary + external_operation_ref)."""
    import time as time_mod

    if backend == "memory":
        storage = InMemoryLedgerStorage()
    elif backend == "file":
        storage = FileLedgerStorage(tmp_path / "reconcile_race_expired.json")
    elif backend == "sqlite":
        storage = SqliteLedgerStorage(tmp_path / "reconcile_race_expired.db")
    elif backend == "redis":
        fakeredis = pytest.importorskip("fakeredis")
        fake = fakeredis.FakeRedis(decode_responses=True)
        import redis as redis_mod

        monkeypatch.setattr(redis_mod.Redis, "from_url", lambda url, **kw: fake)
        storage = RedisLedgerStorage("redis://test")

    class NotExecutedReconciler:
        def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
            return ReconcileResult.not_executed()

    ledger_inst = ActionLedger(
        storage=storage,
        reconciler=NotExecutedReconciler(),
        poll_interval=0.01,
        poll_timeout=2.0,
    )
    binding = _BINDING
    request_id = "race-reconcile-not-exec-expired"

    _seed_owner = str(uuid.uuid4())
    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="test_tool",
            args=[],
            kwargs={},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            owner=_seed_owner,
            lease_until=time_mod.time() - 1,
            side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
            external_operation_ref="pi_expired_race",
            idempotency_key=request_id,
        )
    )

    exec_count = 0
    results: list[tuple[str, Any]] = []
    barrier = threading.Barrier(2, timeout=5)

    def _racer() -> None:
        nonlocal exec_count
        barrier.wait()
        entry = ledger_inst.claim_side_effecting(
            request_id,
            "test_tool",
            (),
            {},
            binding,
        )
        if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
            results.append(("completed", entry.result))
        elif entry.terminal_outcome == TerminalOutcome.IN_FLIGHT.value:
            exec_count += 1
            result_value = {"ran": True}
            ledger_inst.complete(request_id, result_value)
            results.append(("ran_tool", result_value))
        else:
            results.append((entry.terminal_outcome or "unknown", None))

    t1 = threading.Thread(target=_racer, daemon=True)
    t2 = threading.Thread(target=_racer, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert exec_count == 1, f"expected exactly 1 tool execution, got {exec_count}"
    statuses = [r[0] for r in results]
    assert "ran_tool" in statuses, f"nobody ran the tool: {results}"
    assert "completed" in statuses, f"nobody saw the completed result: {results}"
    result_values = [r[1] for r in results]
    assert result_values[0] == result_values[1], (
        f"both threads should see the same result: {results}"
    )


# ---------------------------------------------------------------------------
# Tool-level E2E: failed-outcome recording must not mask tool exception
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Stale-snapshot guard in _raise_hard_block (atomicity contract, v1.18+)
# ---------------------------------------------------------------------------


def test_raise_hard_block_stale_snapshot_returns_inflight_held_lease() -> None:
    """A hard-block decision made from a stale snapshot must never call
    ``mark_blocked`` on an entry whose lease is currently held by another
    worker: ``_raise_hard_block`` re-reads the durable record, sees
    ``IN_FLIGHT`` with a live lease, and returns to the poll loop instead
    of raising. (README § Atomicity contract: "the same stale-snapshot guard
    applies in _raise_hard_block".)
    """
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(storage=storage)
    request_id = "stale-hard-block-held"
    now = time.time()

    # Current durable truth: worker-A holds a live lease.
    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="test_tool",
            args=(),
            kwargs={},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            owner="worker-A",
            lease_until=now + 3600,
            side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
            idempotency_key=request_id,
        )
    )

    # Stale snapshot the resolver saw before the lease was renewed: EXPIRED.
    stale = LedgerEntry(
        request_id=request_id,
        tool="test_tool",
        args=(),
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.EXPIRED.value,
        owner="worker-A",
        lease_until=now - 10,
        side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
        idempotency_key=request_id,
    )

    # _reconcile_cas_lost is a thread-local CAS-race latch the claim loop reads;
    # never leak it into later tests (same hygiene as test_property_transitions).
    from mycelium import action_ledger as _action_ledger

    _action_ledger._reconcile_cas_lost.val = False
    try:
        result = ledger_inst._raise_hard_block(request_id, "test_tool", stale, binding=_BINDING)
        assert result.terminal_outcome == TerminalOutcome.IN_FLIGHT.value
        assert result.owner == "worker-A"

        # mark_blocked was never applied: the held entry stays IN_FLIGHT with its
        # live lease intact.
        stored = storage.get(request_id)
        assert stored is not None
        assert stored.terminal_outcome == TerminalOutcome.IN_FLIGHT.value
        assert stored.lease_until == now + 3600
    finally:
        _action_ledger._reconcile_cas_lost.val = False


# ---------------------------------------------------------------------------
# Fencing tokens (Kleppmann monotonic fence)
# ---------------------------------------------------------------------------


def test_fence_round_trips_and_defaults_to_zero() -> None:
    """fence survives to_dict/from_dict; a row without a fence loads as 0."""
    entry = _make_entry(request_id="fence-serde")
    entry = _replace_fence(entry, 7)
    assert entry.to_dict()["fence"] == 7
    assert LedgerEntry.from_dict(entry.to_dict()).fence == 7

    legacy = entry.to_dict()
    del legacy["fence"]
    assert LedgerEntry.from_dict(legacy).fence == 0


def test_claim_bumps_fence_monotonically(ledger: ActionLedger) -> None:
    """First claim carries fence 1; a reclaim of a reclaimable prior bumps to 2.

    Driven at the storage layer so the check is independent of the higher-level
    reclaim policy (a NON_IDEMPOTENT_MUTATE hard-blocks rather than reclaims —
    that safety gate is orthogonal to the monotonic fence).
    """
    request_id = "fence-bump"
    storage = ledger._storage

    outcome, _ = storage.try_claim_inflight(_make_entry(request_id), lease_ttl=30.0)
    assert outcome == "claimed"
    assert storage.get(request_id).fence == 1

    # A previously-failed entry is reclaimable; the next claim bumps the fence.
    ledger.fail(request_id, RuntimeError("boom"))
    outcome, _ = storage.try_claim_inflight(_make_entry(request_id), lease_ttl=30.0)
    assert outcome == "claimed"
    reclaimed = storage.get(request_id)
    assert reclaimed is not None
    assert reclaimed.fence == 2


def test_stale_fence_rejected_even_when_lease_valid(ledger: ActionLedger) -> None:
    """A write carrying a stale fence is refused even though the lease is HELD.

    This is the core fencing guarantee: lease-based reasoning would allow the
    write (lease still valid), but the stored fence has moved on, so the CAS
    rejects it.
    """
    request_id = "fence-stale-vs-lease"
    now = time.time()
    # Stored entry: fence 2, lease firmly HELD (an hour out).
    _set_entry_on_storage(
        ledger,
        request_id,
        LedgerEntry(
            request_id=request_id,
            tool="test_tool",
            args=(),
            kwargs={},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            fence=2,
            owner="worker-B",
            lease_until=now + 3600,
            idempotency_key=request_id,
            effect_protocol_required=True,
        ),
    )
    # Stale worker holds fence 1 (its claim was superseded).
    with pytest.raises(LedgerError, match="requires the claim fence"):
        ledger.complete(request_id, {"stale": True})
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(
            request_id,
            {"stale": True},
            _expected_owner="worker-B",
            _expected_fence=1,
        )
    stored = ledger.get(request_id)
    assert stored is not None
    assert stored.terminal_outcome == TerminalOutcome.IN_FLIGHT.value
    assert stored.fence == 2


def test_resumed_stale_worker_cannot_commit_after_takeover(
    ledger: ActionLedger,
) -> None:
    """Worker A claims, its slot is reclaimed by worker B (fence bumps). A resumes
    and cannot complete/commit its effect — rejected by fence, not by clock."""
    request_id = "fence-takeover"
    storage = ledger._storage

    # Worker A wins the first claim (fence 1).
    storage.try_claim_inflight(_make_entry(request_id), lease_ttl=30.0)
    a_entry = storage.get(request_id)
    assert a_entry is not None and a_entry.fence == 1
    a_owner = a_entry.owner

    # A's slot becomes reclaimable; worker B reclaims and bumps to fence 2.
    ledger.fail(request_id, RuntimeError("A appeared dead"))
    b_owner = "worker-B"
    storage.try_claim_inflight(_make_entry(request_id, owner=b_owner), lease_ttl=30.0)
    b_entry = storage.get(request_id)
    assert b_entry is not None and b_entry.fence == 2

    # A resumes and tries to commit with its stale fence — refused.
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(
            request_id,
            {"from": "A"},
            _expected_owner=a_owner,
            _expected_fence=1,
        )

    # B (current holder, fence 2) can commit.
    ledger.complete(
        request_id,
        {"from": "B"},
        _expected_owner=b_entry.owner,
        _expected_fence=b_entry.fence,
    )
    final = ledger.get(request_id)
    assert final is not None
    assert final.terminal_outcome == TerminalOutcome.COMPLETED.value
    assert final.result == {"from": "B"}


def test_resumed_stale_worker_cannot_cross_provider_boundary(
    ledger: ActionLedger,
) -> None:
    request_id = "fence-before-provider"
    storage = ledger._storage
    storage.try_claim_inflight(_make_entry(request_id), lease_ttl=30.0)
    worker_a = storage.get(request_id)
    assert worker_a is not None
    ledger.fail(request_id, RuntimeError("A appeared dead"))
    storage.try_claim_inflight(_make_entry(request_id, owner="worker-B"), lease_ttl=30.0)

    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.advance_boundary(
            request_id,
            SideEffectBoundary.MAYBE_CROSSED,
            expected_owner=worker_a.owner,
            expected_fence=worker_a.fence,
        )

    assert storage.get(request_id).side_effect_boundary == (SideEffectBoundary.NOT_CROSSED.value)


def test_auto_renew_does_not_adopt_successor_fence() -> None:
    from dataclasses import replace

    from mycelium.action_ledger import _lease_auto_renew

    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(
        storage=storage,
        lease_ttl=0.03,
        lease_renew_interval=0.01,
    )
    worker_a = replace(
        _make_entry("fence-auto-renew", owner="worker-A"),
        fence=1,
        lease_until=time.time() + 10,
    )
    storage.set(worker_a)
    successor = replace(
        worker_a,
        owner="worker-B",
        fence=2,
        lease_until=time.time() + 20,
        last_heartbeat_at=123.0,
    )

    with _lease_auto_renew(
        ledger,
        worker_a.request_id,
        owner=worker_a.owner,
        fence=worker_a.fence,
    ):
        storage.set(successor)
        time.sleep(0.05)

    assert storage.get(worker_a.request_id) == successor


def test_poll_timeout_cannot_mark_successor_unknown() -> None:
    from dataclasses import replace

    storage = InMemoryLedgerStorage()

    class TakeoverBeforeUnknownLedger(ActionLedger):
        def mark_unknown(self, request_id: str, **kwargs: Any) -> LedgerEntry:
            current = storage.get(request_id)
            assert current is not None
            storage.set(
                replace(
                    current,
                    owner="worker-B",
                    fence=current.fence + 1,
                    lease_until=time.time() + 30,
                )
            )
            return super().mark_unknown(request_id, **kwargs)

    ledger = TakeoverBeforeUnknownLedger(storage=storage)
    observed = replace(
        _make_entry("fence-poll-timeout", owner="worker-A"),
        fence=1,
        lease_until=time.time() + 30,
    )
    storage.set(observed)

    ledger._poll_side_effecting(
        observed.request_id,
        tool=observed.tool,
        interval=0.01,
        poll_deadline=time.time() - 1,
    )

    successor = storage.get(observed.request_id)
    assert successor is not None
    assert successor.owner == "worker-B"
    assert successor.fence == 2
    assert successor.terminal_outcome == TerminalOutcome.IN_FLIGHT.value


def test_delayed_reconcile_cannot_complete_successor() -> None:
    from dataclasses import replace

    started = threading.Event()
    finish = threading.Event()

    class DelayedCompletedReconciler:
        def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
            started.set()
            assert finish.wait(timeout=2)
            return ReconcileResult.completed({"provider": "worker-A"})

    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage, reconciler=DelayedCompletedReconciler())
    observed = replace(
        _make_entry(
            "fence-delayed-reconcile",
            terminal_outcome=TerminalOutcome.BLOCKED.value,
            owner="worker-A",
        ),
        fence=1,
        external_operation_ref="provider-A",
    )
    storage.set(observed)
    results: list[LedgerEntry | None] = []

    def reconcile() -> None:
        results.append(
            ledger._attempt_reconcile(
                observed.request_id,
                observed.tool,
                (),
                {},
                observed,
                _BINDING,
            )
        )

    thread = threading.Thread(target=reconcile, daemon=True)
    thread.start()
    assert started.wait(timeout=2)
    successor = replace(
        observed,
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        owner="worker-B",
        fence=2,
        lease_until=time.time() + 30,
        external_operation_ref=None,
    )
    storage.set(successor)
    finish.set()
    thread.join(timeout=2)

    assert results == [successor]
    assert storage.get(observed.request_id) == successor


def test_matching_fence_allows_single_worker_flow(ledger: ActionLedger) -> None:
    """The uncontended single-worker path completes normally under fencing."""
    request_id = "fence-happy"
    binding = _BINDING
    kwargs = {"request_id": request_id, "thread_id": "t", "run_id": "r"}
    with _scope():
        claimed = ledger.claim_side_effecting(
            request_id, "charge", (1,), dict(kwargs), binding, lease_ttl=30.0
        )
    ledger.record_decision(
        request_id,
        {"allowed": True, "verdicts": [], "denied_reasons": []},
        expected_owner=claimed.owner,
        expected_fence=claimed.fence,
    )
    ledger.complete(
        request_id,
        {"ok": True},
        _expected_owner=claimed.owner,
        _expected_fence=claimed.fence,
    )
    stored = ledger.get(request_id)
    assert stored is not None
    assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value
    assert stored.result == {"ok": True}


def test_wrapper_single_worker_flow_unaffected_by_fence(ledger: ActionLedger) -> None:
    """End-to-end @ledger flow through the wrapper is unchanged by fencing."""
    from mycelium.action_ledger import ledger_sync

    storage = ledger._storage
    calls: list[int] = []

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, Any]:
        calls.append(amount)
        return {"charged": amount}

    with _scope():
        result = charge(5, request_id="fence-wrapper")
    assert result == {"charged": 5}
    assert calls == [5]
    stored = ledger.get("fence-wrapper")
    assert stored is not None
    assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value
    assert stored.fence == 1
