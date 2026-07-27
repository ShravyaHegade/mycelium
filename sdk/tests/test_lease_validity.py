"""Lease validity is first-class in resolution (not part of transition_key)."""

from __future__ import annotations

import asyncio
import threading
import time
import warnings
from dataclasses import replace

import pytest

from mycelium import (
    ActionLedger,
    InMemoryLedgerStorage,
    LeaseValidity,
    LedgerEntry,
    LedgerError,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
    renew_lease,
    resolve_lease_validity,
)
from mycelium.transition_resolution import TransitionGate, resolve_side_effect_gate


def test_resolve_lease_validity_held_expired_unbounded() -> None:
    now = 1_000_000.0
    assert resolve_lease_validity(None, now=now) == LeaseValidity.UNBOUNDED
    assert resolve_lease_validity(now + 10, now=now) == LeaseValidity.HELD
    assert resolve_lease_validity(now - 1, now=now) == LeaseValidity.EXPIRED
    assert resolve_lease_validity(now, now=now) == LeaseValidity.EXPIRED


def test_entry_lease_validity_drives_terminal_outcome() -> None:
    held = LedgerEntry(
        request_id="k1",
        tool="charge",
        args=[],
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        lease_until=time.time() + 60,
    )
    assert held.lease_validity() == LeaseValidity.HELD
    assert held.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT

    stale = LedgerEntry(
        request_id="k2",
        tool="charge",
        args=[],
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        lease_until=time.time() - 1,
    )
    assert stale.lease_validity() == LeaseValidity.EXPIRED
    assert stale.resolved_terminal_outcome() == TerminalOutcome.EXPIRED


def test_renew_lease_keeps_held_and_poll_gate() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage, lease_ttl=0.05)
    claimed = ledger.claim("pay-1", "send_payment", (), {"amount": 1})
    old_until = claimed.lease_until
    assert old_until is not None

    renewed = ledger.renew_lease("pay-1", lease_ttl=3600.0)
    assert renewed.lease_until is not None
    assert renewed.lease_until > old_until
    assert renewed.lease_validity() == LeaseValidity.HELD
    assert renewed.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT

    binding = ToolTransitionBinding.for_tool(
        agent_id="a",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )
    assert resolve_side_effect_gate(renewed, binding) == TransitionGate.POLL


def test_renew_lease_rejects_already_expired() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage, lease_ttl=3600.0)
    ledger.claim("pay-2", "send_payment", (), {})
    existing = storage.get("pay-2")
    assert existing is not None
    storage.set(replace(existing, lease_until=time.time() - 1))

    with pytest.raises(LedgerError, match="already expired"):
        ledger.renew_lease("pay-2")


def test_renew_lease_rejects_completed() -> None:
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage)
    ledger.claim("pay-3", "send_payment", (), {})
    ledger.complete("pay-3", {"ok": True})
    with pytest.raises(LedgerError, match="not IN_FLIGHT"):
        ledger.renew_lease("pay-3")


def test_module_renew_lease_inside_ledgered_tool() -> None:
    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="agent",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )
    seen: dict[str, float | None] = {}

    @ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=0.05,
    )
    def slow_charge(amount: float) -> dict:
        entries = storage.list_all()
        assert len(entries) == 1
        old = entries[0].lease_until
        renew_lease(lease_ttl=3600.0)
        after = storage.get(entries[0].request_id)
        assert after is not None
        assert after.lease_until is not None
        assert old is not None
        assert after.lease_until > old
        seen["extended"] = after.lease_until
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1", node="n1")):
        assert slow_charge(10.0) == {"charged": 10.0}
    assert seen["extended"] is not None


def test_module_renew_lease_outside_tool_warns() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        renew_lease()
    assert any("outside a ledgered tool" in str(w.message) for w in caught)


def test_auto_renew_keeps_lease_held_past_original_ttl() -> None:
    """Decorator heartbeat extends lease_until while the tool body runs."""
    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="agent",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )
    observed: dict[str, float | None] = {}

    @ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=0.08,
        lease_renew_interval=0.02,
    )
    def slow_charge(amount: float) -> dict:
        entries = storage.list_all()
        assert len(entries) == 1
        claimed_until = entries[0].lease_until
        assert claimed_until is not None
        observed["claimed_until"] = claimed_until
        # Outlast the original lease window; auto-renew must keep it HELD.
        time.sleep(0.22)
        after = storage.get(entries[0].request_id)
        assert after is not None
        assert after.lease_until is not None
        assert after.lease_until > claimed_until
        assert after.lease_validity() == LeaseValidity.HELD
        observed["mid_flight_until"] = after.lease_until
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1", node="n1")):
        assert slow_charge(10.0) == {"charged": 10.0}
    assert observed["mid_flight_until"] is not None
    assert observed["claimed_until"] is not None
    assert observed["mid_flight_until"] > observed["claimed_until"]


def test_auto_renew_keeps_peer_on_poll_past_original_ttl() -> None:
    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="agent",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )
    started = threading.Event()
    release = threading.Event()
    result_box: list[dict] = []
    gate_box: list[TransitionGate] = []
    errors: list[BaseException] = []

    @ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=0.08,
        lease_renew_interval=0.02,
    )
    def slow_charge(amount: float) -> dict:
        started.set()
        assert release.wait(timeout=2.0)
        return {"charged": amount}

    def run_owner() -> None:
        try:
            with execution_scope(TransitionScope(thread_id="t1", run_id="r1", node="n1")):
                result_box.append(slow_charge(7.0))
        except BaseException as exc:  # noqa: BLE001 — surface in parent thread
            errors.append(exc)

    def run_peer() -> None:
        try:
            assert started.wait(timeout=2.0)
            time.sleep(0.12)  # past original lease_ttl without auto-renew → EXPIRED
            entries = storage.list_all()
            assert len(entries) == 1
            assert entries[0].lease_validity() == LeaseValidity.HELD
            gate_box.append(resolve_side_effect_gate(entries[0], binding))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            release.set()

    owner = threading.Thread(target=run_owner)
    peer = threading.Thread(target=run_peer)
    owner.start()
    peer.start()
    peer.join(timeout=2.0)
    owner.join(timeout=2.0)

    assert errors == []
    assert result_box == [{"charged": 7.0}]
    assert gate_box == [TransitionGate.POLL]


def test_auto_renew_disabled_when_interval_zero() -> None:
    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="agent",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )

    @ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=0.05,
        lease_renew_interval=0,
    )
    def slow_charge(amount: float) -> dict:
        entries = storage.list_all()
        claimed_until = entries[0].lease_until
        assert claimed_until is not None
        time.sleep(0.12)
        after = storage.get(entries[0].request_id)
        assert after is not None
        # No heartbeat → wall clock past original lease_until.
        assert after.lease_until == claimed_until
        assert after.lease_validity() == LeaseValidity.EXPIRED
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1", node="n1")):
        assert slow_charge(3.0) == {"charged": 3.0}


async def test_auto_renew_async_ledger() -> None:
    from mycelium import ledger

    storage = InMemoryLedgerStorage()
    binding = ToolTransitionBinding.for_tool(
        agent_id="agent",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )

    @ledger(
        storage=storage,
        transition_binding=binding,
        lease_ttl=0.08,
        lease_renew_interval=0.02,
    )
    async def slow_charge(amount: float) -> dict:
        entries = storage.list_all()
        claimed_until = entries[0].lease_until
        assert claimed_until is not None
        await asyncio.sleep(0.22)
        after = storage.get(entries[0].request_id)
        assert after is not None
        assert after.lease_until is not None
        assert after.lease_until > claimed_until
        assert after.lease_validity() == LeaseValidity.HELD
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1", node="n1")):
        assert await slow_charge(10.0) == {"charged": 10.0}
