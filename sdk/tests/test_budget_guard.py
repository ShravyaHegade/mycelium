"""Tests for BudgetGuard (cost / time / step ceilings)."""

from __future__ import annotations

import threading
import time

import pytest

from mycelium.action_ledger import (
    LedgerAlreadyResolvedError,
    LedgerHardBlockError,
    LedgerReleaseRefusedError,
)
from mycelium.budget_guard import (
    KIND_LLM,
    KIND_TOOL,
    ON_MISSING_HARD,
    BudgetGuard,
    FileBudgetGuardStorage,
    InMemoryBudgetGuardStorage,
    SqliteBudgetGuardStorage,
    budget_guard_sync,
    parse_duration_seconds,
)
from mycelium.loop_guard import (
    VERIFIED_ABORT_RUN,
    VERIFIED_ALLOW_ONCE,
    VERIFIED_CLEAR,
)
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import TransitionScope, execution_scope


def _scope(run_id: str = "run-1") -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id=run_id, node="tools")


def test_parse_duration_seconds() -> None:
    assert parse_duration_seconds(30) == 30.0
    assert parse_duration_seconds("15m") == 900.0
    assert parse_duration_seconds("1h") == 3600.0
    with pytest.raises(ValueError):
        parse_duration_seconds("nope")


def test_max_steps_soft_then_hard() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=5, warn_at=0.8)
    calls = {"n": 0}

    @budget_guard_sync(guard)
    def search(q: str, *, tool_call_id: str) -> str:
        calls["n"] += 1
        return q

    with execution_scope(_scope()):
        for i in range(3):
            assert search(q="foo", tool_call_id=f"c{i}") == "foo"
        assert calls["n"] == 3

        with pytest.raises(ToolBoundaryError) as soft:
            search(q="foo", tool_call_id="c3")
        assert soft.value.violation == "budget_warning"
        assert calls["n"] == 3

        assert search(q="foo", tool_call_id="c4") == "foo"
        assert search(q="foo", tool_call_id="c5") == "foo"
        assert calls["n"] == 5

        with pytest.raises(LedgerHardBlockError):
            search(q="foo", tool_call_id="c6")
        assert calls["n"] == 5


def test_llm_check_and_record_usage_usd() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(),
        max_usd=1.0,
        warn_at=0.8,
        on_missing_meter=ON_MISSING_HARD,
    )
    with execution_scope(_scope("burn")):
        guard.check(KIND_LLM)
        guard.record_usage(usd=0.5)
        remaining = guard.remaining_budget("burn")
        assert remaining is not None
        assert remaining.usd == pytest.approx(0.5)

        guard.check(KIND_LLM)
        guard.record_usage(usd=0.6)  # crosses 1.0; overshoot mid-call ok
        state = guard.get_state("burn")
        assert state is not None
        assert state.hard_blocked
        assert state.blocked_dimension == "max_usd"

        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_LLM)


def test_missing_meter_fail_closed() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(),
        max_tokens=1000,
        max_steps=10,
        on_missing_meter=ON_MISSING_HARD,
    )
    with execution_scope(_scope("nometer")):
        guard.check(KIND_LLM)
        with pytest.raises(LedgerHardBlockError) as exc:
            guard.check(KIND_LLM)
        assert "record_usage was never called" in str(exc.value)


def test_max_duration_hard_block() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(),
        max_duration=0.05,
        warn_at=1.0,  # skip soft path
    )
    with execution_scope(_scope("slow")):
        guard.check(KIND_TOOL, increment_steps=True)
        time.sleep(0.06)
        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_TOOL, increment_steps=True)


def test_release_clear_and_allow_once() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=2, warn_at=1.0)
    with execution_scope(_scope("rel")):
        guard.check(KIND_TOOL)
        guard.check(KIND_TOOL)
        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_TOOL)

        state = guard.release(
            "rel",
            verified=VERIFIED_ALLOW_ONCE,
            by="ops@example.com",
            reason="one more step",
        )
        assert state.allow_once
        assert not state.hard_blocked

        guard.check(KIND_TOOL)  # allow-once consumes
        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_TOOL)

        # re-block clears prior resolution so clear can run again
        state = guard.release(
            "rel",
            verified=VERIFIED_CLEAR,
            by="ops@example.com",
            reason="reset runway",
        )
        assert state.steps == 0
        assert not state.hard_blocked
        guard.check(KIND_TOOL)


def test_release_refuses_when_not_blocked() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=10)
    with execution_scope(_scope("ok")):
        guard.check(KIND_TOOL)
    with pytest.raises(LedgerReleaseRefusedError):
        guard.release(
            "ok",
            verified=VERIFIED_CLEAR,
            by="ops",
            reason="noop",
        )


def test_atomic_update_no_double_step(tmp_path) -> None:
    storage = FileBudgetGuardStorage(tmp_path / "budget.json")
    guard = BudgetGuard(storage, max_steps=100, warn_at=1.0)
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            with execution_scope(_scope("race")):
                for _ in range(20):
                    guard.check(KIND_TOOL)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    state = guard.get_state("race")
    assert state is not None
    assert state.steps == 80


def test_sqlite_storage_update(tmp_path) -> None:
    storage = SqliteBudgetGuardStorage(tmp_path / "budget.db")
    guard = BudgetGuard(storage, max_steps=3, warn_at=1.0)
    with execution_scope(_scope("sql")):
        guard.check(KIND_TOOL)
        guard.check(KIND_TOOL)
        guard.check(KIND_TOOL)
        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_TOOL)
    again = BudgetGuard(
        SqliteBudgetGuardStorage(tmp_path / "budget.db"),
        max_steps=3,
        warn_at=1.0,
    )
    state = again.get_state("sql")
    assert state is not None
    assert state.hard_blocked


def test_yaml_budget_wires_decorator() -> None:
    from mycelium.config import load_config_from_string

    cfg = load_config_from_string(
        """
budget:
  storage: memory
  max_steps: 2
  warn_at: 1.0
tools:
  ping:
    ledger: false
"""
    )
    guard = cfg.build_budget_guard()
    assert guard is not None

    calls = {"n": 0}

    @cfg.apply
    def ping(*, tool_call_id: str) -> str:
        calls["n"] += 1
        return "pong"

    with execution_scope(_scope("yaml")):
        assert ping(tool_call_id="a") == "pong"
        assert ping(tool_call_id="b") == "pong"
        with pytest.raises(LedgerHardBlockError):
            ping(tool_call_id="c")
    assert calls["n"] == 2


def test_already_resolved_after_abort() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=1, warn_at=1.0)
    with execution_scope(_scope("ab")):
        guard.check(KIND_TOOL)
        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_TOOL)
    guard.release(
        "ab",
        verified=VERIFIED_ABORT_RUN,
        by="ops",
        reason="kill",
    )
    with pytest.raises(LedgerAlreadyResolvedError):
        guard.release(
            "ab",
            verified=VERIFIED_CLEAR,
            by="ops",
            reason="retry",
        )
