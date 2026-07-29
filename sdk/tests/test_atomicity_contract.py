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

import pytest

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerOutcomeAlreadySetError,
    RedisLedgerStorage,
    SideEffectClass,
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


def _set_entry_on_storage(
    ledger: ActionLedger, request_id: str, entry: LedgerEntry
) -> None:
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
        pytest.param("redis", id="redis"),
    ]
)
def ledger(request, tmp_path, monkeypatch):
    if request.param == "memory":
        storage = InMemoryLedgerStorage()
    elif request.param == "file":
        storage = FileLedgerStorage(tmp_path / "ledger.json")
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
def test_release_completed_from_terminal_outcomes(
    ledger: ActionLedger, from_outcome: str
) -> None:
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
    assert len(errors) == 1, (
        f"expected exactly one refusal, got {len(errors)}"
    )
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
# Tool-level E2E: failed-outcome recording must not mask tool exception
# ---------------------------------------------------------------------------


