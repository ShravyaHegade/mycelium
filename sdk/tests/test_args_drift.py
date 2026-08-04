"""Opt-in args-drift / identity-conflict gate (AF-002 Ring 3).

Default ``on_args_drift="off"`` preserves the intentional contract pinned by
``test_semantic_identity``: same dispatch ticket + different args is a new
transition. When enabled, Mycelium rejects the mismatch.
"""

from __future__ import annotations

import pytest

from mycelium import (
    ARGS_DRIFT_HARD,
    ARGS_DRIFT_SOFT,
    InMemoryLedgerStorage,
    LedgerHardBlockError,
    SideEffectClass,
    ToolBoundaryError,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
)

_BINDING = ToolTransitionBinding.for_tool(
    agent_id="args-drift",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)


def test_default_off_allows_same_request_id_different_args() -> None:
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    charge(amount=10, request_id="intent-1")
    charge(amount=11, request_id="intent-1")
    assert executions == [10, 11]


def test_hard_blocks_same_request_id_different_args() -> None:
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_HARD,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    charge(amount=10, request_id="intent-1")
    with pytest.raises(LedgerHardBlockError, match="identity conflict"):
        charge(amount=11, request_id="intent-1")
    assert executions == [10]


def test_soft_raises_tool_boundary_error() -> None:
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_SOFT,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    charge(amount=10, request_id="intent-1")
    with pytest.raises(ToolBoundaryError, match="identity conflict"):
        charge(amount=11, request_id="intent-1")
    assert executions == [10]


def test_hard_blocks_same_tool_call_id_different_args() -> None:
    """Dispatch ticket via tool_call_id under different transition keys."""
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_HARD,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    charge(amount=10, tool_call_id="tc-1")
    with pytest.raises(LedgerHardBlockError, match="identity conflict"):
        charge(amount=11, tool_call_id="tc-1")
    assert executions == [10]


def test_same_args_idempotent_return_still_works() -> None:
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_HARD,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    assert charge(amount=10, request_id="intent-1") == 10
    assert charge(amount=10, request_id="intent-1") == 10
    assert executions == [10]


def test_new_dispatch_id_allowed() -> None:
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_HARD,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    charge(amount=10, tool_call_id="tc-1")
    charge(amount=11, tool_call_id="tc-2")
    assert executions == [10, 11]


def test_legacy_same_request_id_hard_block() -> None:
    """Without transition binding, request_id is the storage key."""
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(storage=storage, on_args_drift=ARGS_DRIFT_HARD)
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    charge(amount=10, request_id="rid-1")
    with pytest.raises(LedgerHardBlockError, match="identity conflict"):
        charge(amount=11, request_id="rid-1")
    assert executions == [10]


def test_invalid_on_args_drift_rejected() -> None:
    with pytest.raises(ValueError, match="on_args_drift"):
        @ledger_sync(on_args_drift="maybe")
        def charge(amount: int) -> int:
            return amount


def test_different_runs_same_ticket_allowed() -> None:
    """Args-drift is run-isolated: same tool_call_id in another run is fine."""
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_HARD,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    with execution_scope(TransitionScope(run_id="run-a", thread_id="t")):
        charge(amount=10, tool_call_id="tc-1")
    with execution_scope(TransitionScope(run_id="run-b", thread_id="t")):
        charge(amount=11, tool_call_id="tc-1")
    assert executions == [10, 11]


def test_same_run_same_ticket_still_blocks() -> None:
    """Within one run, same tool_call_id + different args still conflicts."""
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_HARD,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    with execution_scope(TransitionScope(run_id="run-a", thread_id="t")):
        charge(amount=10, tool_call_id="tc-1")
        with pytest.raises(LedgerHardBlockError, match="identity conflict"):
            charge(amount=11, tool_call_id="tc-1")
    assert executions == [10]