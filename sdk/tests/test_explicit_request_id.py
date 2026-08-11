"""Host-owned ``request_id`` is the transition identity when provided."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from mycelium import (
    ActionLedger,
    InMemoryLedgerStorage,
    LedgerHardBlockError,
    SideEffectBoundary,
    SideEffectClass,
    SqliteLedgerStorage,
    ToolBoundaryError,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger,
    ledger_sync,
    parse_explicit_request_id,
    side_effect,
)
from mycelium.action_ledger import get_active_transition

_BINDING = ToolTransitionBinding.for_tool(
    agent_id="explicit-rid",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)

_BUSINESS_ID = "charge-order:ORD-123"


def test_parse_explicit_request_id_rejects_empty_and_non_strings() -> None:
    assert parse_explicit_request_id({}) is None
    assert parse_explicit_request_id({"request_id": _BUSINESS_ID}) == _BUSINESS_ID
    with pytest.raises(ValueError, match="non-empty string"):
        parse_explicit_request_id({"request_id": ""})
    with pytest.raises(ValueError, match="non-empty string"):
        parse_explicit_request_id({"request_id": "   "})
    with pytest.raises(ValueError, match="non-empty string"):
        parse_explicit_request_id({"request_id": None})
    with pytest.raises(ValueError, match="non-empty string"):
        parse_explicit_request_id({"request_id": 123})


def test_first_call_executes_once_and_retry_returns_stored() -> None:
    executions: list[int] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        executions.append(amount)
        return {"charged": amount}

    first = charge(amount=10, request_id=_BUSINESS_ID)
    replay = charge(amount=10, request_id=_BUSINESS_ID)
    assert first == replay == {"charged": 10}
    assert executions == [10]


def test_request_id_not_forwarded_to_wrapped_function() -> None:
    seen: list[dict[str, object]] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge(amount: int, **kwargs: object) -> dict[str, int]:
        seen.append(dict(kwargs))
        return {"charged": amount}

    charge(amount=10, request_id=_BUSINESS_ID)
    assert seen == [{}]


def test_missing_request_id_keeps_tool_call_id_identity() -> None:
    executions: list[int] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        executions.append(amount)
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        a = charge(amount=10, tool_call_id="call_1")
        b = charge(amount=10, tool_call_id="call_1")
        c = charge(amount=10, tool_call_id="call_2")

    assert a == b == {"charged": 10}
    assert c == {"charged": 10}
    assert executions == [10, 10]


def test_empty_request_id_rejected_before_body_runs() -> None:
    executions: list[int] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        executions.append(amount)
        return {"charged": amount}

    with pytest.raises(ValueError, match="non-empty string"):
        charge(amount=10, request_id="")
    assert executions == []


def test_same_id_changed_args_is_args_drift() -> None:
    executions: list[int] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        executions.append(amount)
        return {"charged": amount}

    charge(amount=10, request_id=_BUSINESS_ID)
    with pytest.raises(ToolBoundaryError, match="identity conflict") as exc:
        charge(amount=11, request_id=_BUSINESS_ID)
    assert exc.value.violation == "args_drift"
    assert executions == [10]


def test_same_id_different_tool_fail_closed() -> None:
    storage = InMemoryLedgerStorage()

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        return {"charged": amount}

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def refund(amount: int) -> dict[str, int]:
        return {"refunded": amount}

    charge(amount=10, request_id=_BUSINESS_ID)
    with pytest.raises(ToolBoundaryError, match="identity conflict"):
        refund(amount=10, request_id=_BUSINESS_ID)


def test_same_id_different_scope_fail_closed() -> None:
    executions: list[int] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        executions.append(amount)
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        charge(amount=10, request_id=_BUSINESS_ID)
    with execution_scope(TransitionScope(thread_id="t2", run_id="r2")):
        with pytest.raises(ToolBoundaryError, match="identity conflict"):
            charge(amount=10, request_id=_BUSINESS_ID)
    assert executions == [10]


def test_concurrent_same_id_executes_once() -> None:
    storage = InMemoryLedgerStorage()
    executions: list[int] = []
    started = threading.Event()
    results: list[dict[str, int]] = []

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        started.set()
        time.sleep(0.05)
        executions.append(amount)
        return {"charged": amount}

    def worker() -> None:
        results.append(charge(amount=10, request_id=_BUSINESS_ID))

    first = threading.Thread(target=worker)
    second = threading.Thread(target=worker)
    first.start()
    assert started.wait(timeout=1.0)
    second.start()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert executions == [10]
    assert results == [{"charged": 10}, {"charged": 10}]


def test_sqlite_restart_same_id_does_not_reexecute(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    executions: list[str] = []

    storage1 = SqliteLedgerStorage(db)

    @ledger_sync(storage=storage1, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:
        executions.append("first")
        return {"charged": amount}

    first = charge(amount=10, request_id=_BUSINESS_ID)

    storage2 = SqliteLedgerStorage(db)

    @ledger_sync(storage=storage2, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:  # noqa: F811
        executions.append("second")
        return {"charged": amount}

    replay = charge(amount=10, request_id=_BUSINESS_ID)
    assert first == replay == {"charged": 10}
    assert executions == ["first"]
    stored = storage2.get(_BUSINESS_ID)
    assert stored is not None
    assert stored.request_id == _BUSINESS_ID


def test_sqlite_crash_window_same_id_does_not_reexecute(tmp_path: Path) -> None:
    class _Crash(BaseException):
        pass

    db = tmp_path / "ledger.db"
    executions: list[str] = []
    storage1 = SqliteLedgerStorage(db)

    @ledger_sync(
        storage=storage1,
        transition_binding=_BINDING,
        lease_ttl=0.05,
        lease_renew_interval=0,
    )
    def charge(amount: int) -> dict[str, int]:
        executions.append("first")
        active = get_active_transition()
        assert active is not None
        assert active.request_id == _BUSINESS_ID
        with side_effect():
            raise _Crash("killed mid-effect")

    with pytest.raises(_Crash):
        charge(amount=10, request_id=_BUSINESS_ID)

    crashed = storage1.get(_BUSINESS_ID)
    assert crashed is not None
    assert crashed.side_effect_boundary == SideEffectBoundary.MAYBE_CROSSED.value
    storage1.set(replace(crashed, lease_until=time.time() - 1))

    storage2 = SqliteLedgerStorage(db)

    @ledger_sync(storage=storage2, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, int]:  # noqa: F811
        executions.append("second")
        return {"charged": amount}

    with pytest.raises(LedgerHardBlockError):
        charge(amount=10, request_id=_BUSINESS_ID)
    assert executions == ["first"]


def test_async_matches_sync() -> None:
    sync_exec: list[int] = []
    async_exec: list[int] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge_sync(amount: int) -> dict[str, int]:
        sync_exec.append(amount)
        return {"charged": amount}

    @ledger(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    async def charge_async(amount: int) -> dict[str, int]:
        async_exec.append(amount)
        return {"charged": amount}

    assert charge_sync(amount=10, request_id=_BUSINESS_ID) == {"charged": 10}
    assert charge_sync(amount=10, request_id=_BUSINESS_ID) == {"charged": 10}

    async def run() -> None:
        assert await charge_async(amount=10, request_id=_BUSINESS_ID) == {"charged": 10}
        assert await charge_async(amount=10, request_id=_BUSINESS_ID) == {"charged": 10}

    asyncio.run(run())
    assert sync_exec == async_exec == [10]


def test_derive_request_id_uses_explicit_value() -> None:
    ledger_inst = ActionLedger()
    rid = ledger_inst.derive_request_id(
        "charge",
        (),
        {"amount": 10, "request_id": _BUSINESS_ID},
        transition_binding=_BINDING,
    )
    assert rid == _BUSINESS_ID
    hashed = ledger_inst.derive_request_id(
        "charge",
        (),
        {"amount": 10, "tool_call_id": "call_1"},
        transition_binding=_BINDING,
    )
    assert hashed != _BUSINESS_ID
    assert len(hashed) == 64
