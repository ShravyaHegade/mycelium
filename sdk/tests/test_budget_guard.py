"""Tests for BudgetGuard (cost / time / step ceilings)."""

from __future__ import annotations

import threading
import time
import warnings

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
    BudgetRunState,
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
from mycelium.transition import TransitionScope, execution_scope


def _scope(run_id: str = "run-1") -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id=run_id, node="tools")


def test_parse_duration_seconds() -> None:
    assert parse_duration_seconds(30) == 30.0
    assert parse_duration_seconds("15m") == 900.0
    assert parse_duration_seconds("1h") == 3600.0
    with pytest.raises(ValueError):
        parse_duration_seconds("nope")


def test_budget_run_state_positional_constructor_preserves_updated_at() -> None:
    state = BudgetRunState(
        "compat",
        1.0,
        2,
        3,
        4,
        5.0,
        True,
        {"max_steps": True},
        True,
        "max_steps",
        "clear",
        "ops@example.com",
        "reset",
        6.0,
        True,
        "model",
        "provider",
        7.0,
    )

    assert state.updated_at == 7.0
    assert state.last_check_incremented_steps is None


def test_max_steps_ceiling_allows_n() -> None:
    """Honey Mail 2: max_steps=N must run N bodies (warn_at must not steal one)."""
    for warn_at in (1.0, 0.8):
        guard = BudgetGuard(
            InMemoryBudgetGuardStorage(), max_steps=3, warn_at=warn_at
        )
        calls = {"n": 0}

        @budget_guard_sync(guard)
        def search(q: str, *, tool_call_id: str) -> str:
            calls["n"] += 1
            return q

        with execution_scope(_scope(f"steps-{warn_at}")):
            for i in range(3):
                assert search(q="foo", tool_call_id=f"c{i}") == "foo"
            assert calls["n"] == 3
            state = guard.get_state(f"steps-{warn_at}")
            assert state is not None
            assert state.steps == 3
            with pytest.raises(LedgerHardBlockError):
                search(q="foo", tool_call_id="c3")
            assert calls["n"] == 3


def test_max_steps_soft_warn_allows_then_hard() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=5, warn_at=0.8)
    calls = {"n": 0}

    @budget_guard_sync(guard)
    def search(q: str, *, tool_call_id: str) -> str:
        calls["n"] += 1
        return q

    with execution_scope(_scope()):
        for i in range(4):
            assert search(q="foo", tool_call_id=f"c{i}") == "foo"
        assert calls["n"] == 4

        with pytest.warns(UserWarning, match="approaching max_steps"):
            assert search(q="foo", tool_call_id="c4") == "foo"
        assert calls["n"] == 5
        state = guard.get_state("run-1")
        assert state is not None
        assert state.soft_issued.get("max_steps")

        with pytest.raises(LedgerHardBlockError):
            search(q="foo", tool_call_id="c5")
        assert calls["n"] == 5


def test_manual_max_steps_ceiling_blocks_at_n() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(), max_steps=3, warn_at=1.0, on_missing_meter="off"
    )

    with execution_scope(_scope("manual-steps")):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            for _ in range(3):
                guard.check(KIND_TOOL, increment_steps=False)
                guard.record_usage(steps=1)
        assert not caught

        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_TOOL, increment_steps=False)


def test_warn_at_does_not_refuse_under_ceiling_usd() -> None:
    """Honey Mail 2: warn_at is permissive — $0.85 of $1.00 must not refuse."""
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(),
        max_usd=1.0,
        warn_at=0.8,
        on_missing_meter="off",
    )
    with execution_scope(_scope("usd-warn")):
        guard.record_usage(usd=0.80)
        with pytest.warns(UserWarning, match="approaching max_usd"):
            guard.check(KIND_LLM, increment_steps=False)
        guard.record_usage(usd=0.05)
        state = guard.get_state("usd-warn")
        assert state is not None
        assert state.usd == pytest.approx(0.85)
        assert not state.hard_blocked
        assert state.soft_issued.get("max_usd")

        guard.record_usage(usd=0.20)  # crosses 1.00
        state = guard.get_state("usd-warn")
        assert state is not None
        assert state.hard_blocked
        assert state.blocked_dimension == "max_usd"
        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_LLM, increment_steps=False)


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


def test_record_usage_steps_warns_about_double_metering() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=10, warn_at=1.0)
    with execution_scope(_scope("double-meter")):
        guard.check(KIND_TOOL)
        with pytest.warns(UserWarning, match="auto-meter one step"):
            state = guard.record_usage(steps=1)
    assert state.steps == 2


def test_state_queries_resolve_scope_and_preserve_blocked_runway() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=1, warn_at=1.0)
    with execution_scope(_scope("query-blocked")):
        guard.check(KIND_TOOL)
        with pytest.raises(LedgerHardBlockError):
            guard.check(KIND_TOOL)

        state = guard.get_state()
        remaining = guard.remaining_budget()
        assert state is not None
        assert state.hard_blocked
        assert remaining is not None
        assert remaining.steps == 0

    assert guard.get_state() is None
    assert guard.remaining_budget() is None
    explicit_state = guard.get_state("query-blocked")
    assert explicit_state is not None
    assert explicit_state.to_dict() == state.to_dict()
    explicit_remaining = guard.remaining_budget("query-blocked")
    assert explicit_remaining is not None
    assert explicit_remaining.steps == 0


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
