"""Tests for LoopGuard (AF-003)."""

from __future__ import annotations

import threading

import pytest

from mycelium.action_ledger import (
    LedgerAlreadyResolvedError,
    LedgerHardBlockError,
    LedgerReleaseRefusedError,
)
from mycelium.loop_guard import (
    VERIFIED_ABORT_RUN,
    VERIFIED_ALLOW_ONCE,
    VERIFIED_CLEAR,
    InMemoryLoopGuardStorage,
    LoopGuard,
    action_hash,
    loop_guard_sync,
)
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import SideEffectClass, TransitionScope, execution_scope


def _scope(run_id: str = "run-1") -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id=run_id, node="tools")


def test_action_hash_ignores_dispatch_id() -> None:
    a = action_hash("search", (), {"q": "foo", "tool_call_id": "c1"})
    b = action_hash("search", (), {"q": "foo", "tool_call_id": "c2"})
    assert a == b
    c = action_hash("search", (), {"q": "bar", "tool_call_id": "c3"})
    assert a != c


def test_read_soft_at_n5_then_hard() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str, *, tool_call_id: str) -> str:
        calls["n"] += 1
        return q

    with execution_scope(_scope()):
        for i in range(4):
            assert search(q="foo", tool_call_id=f"c{i}") == "foo"
        assert calls["n"] == 4

        with pytest.raises(ToolBoundaryError) as soft:
            search(q="foo", tool_call_id="c4")
        assert soft.value.violation == "loop_detected"
        assert calls["n"] == 4

        with pytest.raises(LedgerHardBlockError):
            search(q="foo", tool_call_id="c5")
        assert calls["n"] == 4

        # Whole run frozen — other tools too
        @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
        def other(*, tool_call_id: str) -> str:
            calls["n"] += 1
            return "x"

        with pytest.raises(LedgerHardBlockError):
            other(tool_call_id="other1")


def test_mutate_soft_at_n2() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())
    calls = {"n": 0}

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(amount: float, *, tool_call_id: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope()):
        assert charge(10.0, tool_call_id="c0") == 10.0
        with pytest.raises(ToolBoundaryError):
            charge(10.0, tool_call_id="c1")
        assert calls["n"] == 1
        with pytest.raises(LedgerHardBlockError):
            charge(10.0, tool_call_id="c2")


def test_same_dispatch_id_does_not_advance_streak() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())
    calls = {"n": 0}

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(amount: float, *, tool_call_id: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope()):
        charge(10.0, tool_call_id="same")
        charge(10.0, tool_call_id="same")  # retry — must not soft
        charge(10.0, tool_call_id="same")
        assert calls["n"] == 3


def test_missing_scope_skips_guard() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str, *, tool_call_id: str) -> str:
        calls["n"] += 1
        return q

    # No execution_scope — skip (warn once)
    for i in range(10):
        search(q="foo", tool_call_id=f"c{i}")
    assert calls["n"] == 10


def test_release_clear_and_allow_once() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str, *, tool_call_id: str) -> str:
        calls["n"] += 1
        return q

    with execution_scope(_scope("run-rel")):
        for i in range(4):
            search(q="foo", tool_call_id=f"a{i}")
        with pytest.raises(ToolBoundaryError):
            search(q="foo", tool_call_id="a4")
        with pytest.raises(LedgerHardBlockError):
            search(q="foo", tool_call_id="a5")

        state = guard.release(
            "run-rel",
            verified=VERIFIED_ALLOW_ONCE,
            by="ops@example.com",
            reason="one more search ok",
        )
        assert state.allow_once_hash is not None
        assert not state.hard_blocked

        assert search(q="foo", tool_call_id="a6") == "foo"
        assert calls["n"] == 5

        # Re-arms: allow-once left streak=1; three more bodies → streak 4; fifth soft.
        for i in range(3):
            search(q="foo", tool_call_id=f"b{i}")
        with pytest.raises(ToolBoundaryError):
            search(q="foo", tool_call_id="b3")
        with pytest.raises(LedgerHardBlockError):
            search(q="foo", tool_call_id="b4")

        # New hard clears prior resolution stamp — clear is allowed again.
        guard.release(
            "run-rel",
            verified=VERIFIED_CLEAR,
            by="ops@example.com",
            reason="start clean after second hard",
        )
        search(q="foo", tool_call_id="z0")

        # Double release without a new hard is refused.
        state = guard.get_state("run-rel")
        assert state is not None
        state.hard_blocked = True
        state.operator_resolution = VERIFIED_CLEAR
        state.resolved_by = "ops@example.com"
        guard.storage.set(state)
        with pytest.raises(LedgerAlreadyResolvedError):
            guard.release(
                "run-rel",
                verified=VERIFIED_CLEAR,
                by="ops@example.com",
                reason="already resolved",
            )


def test_release_clear_resets_counters() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str, *, tool_call_id: str) -> str:
        return q

    with execution_scope(_scope("run-clear")):
        for i in range(4):
            search(q="foo", tool_call_id=f"c{i}")
        with pytest.raises(ToolBoundaryError):
            search(q="foo", tool_call_id="c4")
        with pytest.raises(LedgerHardBlockError):
            search(q="foo", tool_call_id="c5")

        # Manually clear operator_resolution to simulate new hard cycle after
        # we only test clear from hard — first release is clear.
        state = guard.get_state("run-clear")
        assert state is not None
        # Force unresolved hard for clear path: release clear is first release
        guard.release(
            "run-clear",
            verified=VERIFIED_CLEAR,
            by="ops@example.com",
            reason="start clean",
        )
        for i in range(4):
            search(q="foo", tool_call_id=f"d{i}")
        with pytest.raises(ToolBoundaryError):
            search(q="foo", tool_call_id="d4")


def test_release_abort_keeps_frozen() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(amount: float, *, tool_call_id: str) -> float:
        return amount

    with execution_scope(_scope("run-abort")):
        charge(1.0, tool_call_id="c0")
        with pytest.raises(ToolBoundaryError):
            charge(1.0, tool_call_id="c1")
        with pytest.raises(LedgerHardBlockError):
            charge(1.0, tool_call_id="c2")

        guard.release(
            "run-abort",
            verified=VERIFIED_ABORT_RUN,
            by="ops@example.com",
            reason="stop",
        )
        with pytest.raises(LedgerHardBlockError):
            charge(1.0, tool_call_id="c3")


def test_release_refused_unknown_run() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())
    with pytest.raises(LedgerReleaseRefusedError):
        guard.release(
            "missing",
            verified=VERIFIED_CLEAR,
            by="ops",
            reason="n/a",
        )


def test_file_storage_round_trip(tmp_path) -> None:
    from mycelium.loop_guard import FileLoopGuardStorage

    path = tmp_path / "loop.json"
    guard = LoopGuard(FileLoopGuardStorage(path))

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(amount: float, *, tool_call_id: str) -> float:
        return amount

    with execution_scope(_scope("run-file")):
        charge(1.0, tool_call_id="c0")
        with pytest.raises(ToolBoundaryError):
            charge(1.0, tool_call_id="c1")
        with pytest.raises(LedgerHardBlockError):
            charge(1.0, tool_call_id="c2")

    guard2 = LoopGuard(FileLoopGuardStorage(path))
    state = guard2.get_state("run-file")
    assert state is not None
    assert state.hard_blocked
    with execution_scope(_scope("run-file")):
        with pytest.raises(LedgerHardBlockError):
            @loop_guard_sync(
                guard2, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
            )
            def charge2(amount: float, *, tool_call_id: str) -> float:
                return amount

            charge2(1.0, tool_call_id="c3")


def test_config_apply_orders_loop_guard_outside_ledger() -> None:
    from mycelium.config import load_config_from_string

    yaml_text = """
transition:
  agent_id: a
  policy_version: p
loop_guard:
  storage: memory
  consecutive_soft:
    read: 2
tools:
  search:
    side_effect_class: read
action_ledger:
  storage: memory
  tools: [search]
"""
    config = load_config_from_string(yaml_text)
    calls = {"n": 0}

    def search(**kwargs: object) -> str:
        calls["n"] += 1
        return "ok"

    wrapped = config.apply_tool("search", search)
    with execution_scope(_scope("run-cfg")):
        assert wrapped(tool_call_id="c0") == "ok"
        with pytest.raises(ToolBoundaryError):
            wrapped(tool_call_id="c1")
        assert calls["n"] == 1  # soft: no body


def test_exclude_tool_skips() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage(), exclude=["poll_status"])
    calls = {"n": 0}

    @loop_guard_sync(guard, tool_name="poll_status", side_effect_class=SideEffectClass.READ)
    def poll_status(*, tool_call_id: str) -> str:
        calls["n"] += 1
        return "ok"

    with execution_scope(_scope()):
        for i in range(20):
            poll_status(tool_call_id=f"p{i}")
    assert calls["n"] == 20


def test_concurrent_check_soft_blocks_at_most_once_body_exec() -> None:
    """Atomic update: concurrent identical dispatches share one streak counter."""
    guard = LoopGuard(
        InMemoryLoopGuardStorage(),
        consecutive_soft={SideEffectClass.NON_IDEMPOTENT_MUTATE.value: 2},
    )
    calls = {"n": 0}
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(*, tool_call_id: str) -> str:
        calls["n"] += 1
        return "ok"

    def _worker(i: int) -> None:
        try:
            with execution_scope(_scope("run-race")):
                barrier.wait()
                charge(tool_call_id=f"c{i}")
        except (ToolBoundaryError, LedgerHardBlockError) as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(i,), daemon=True) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    # Threshold 2: at most one body execution before soft/hard; never all 8.
    assert calls["n"] <= 1
    assert len(errors) >= 7
    assert any(isinstance(e, ToolBoundaryError) for e in errors)
