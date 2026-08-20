"""Public wait_for_transition helpers (LangGraph redispatch DX)."""

from __future__ import annotations

import threading
import time

import pytest

from mycelium import (
    InMemoryLedgerStorage,
    LedgerError,
    LedgerPollTimeoutError,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
)


def _ledger(**kwargs):
    from mycelium import ActionLedger

    return ActionLedger(storage=InMemoryLedgerStorage(), **kwargs)


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _allow(ledger, claimed) -> None:
    ledger.record_decision(
        claimed.request_id,
        {"allowed": True, "verdicts": [], "denied_reasons": []},
        expected_fence=claimed.fence,
    )


def test_wait_for_transition_returns_immediately_when_terminal() -> None:
    ledger = _ledger()
    claimed = ledger.claim_side_effecting(
        "r1",
        "charge",
        (),
        {},
        _binding(),
    )
    _allow(ledger, claimed)
    ledger.complete(claimed.request_id, {"ok": True}, expected_fence=claimed.fence)

    entry = ledger.wait_for_transition("r1")
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED
    assert entry.result == {"ok": True}


def test_wait_for_transition_unknown_request_raises() -> None:
    ledger = _ledger()
    with pytest.raises(LedgerError, match="unknown request"):
        ledger.wait_for_transition("missing")


def test_wait_for_transition_times_out_without_mutating() -> None:
    ledger = _ledger(poll_interval=0.01, poll_timeout=0.05)
    ledger.claim_side_effecting("r1", "charge", (), {}, _binding())

    with pytest.raises(LedgerPollTimeoutError):
        ledger.wait_for_transition("r1")

    still = ledger.get("r1")
    assert still is not None
    assert still.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT
    assert still.terminal_outcome == TerminalOutcome.IN_FLIGHT.value


def test_wait_for_transition_sees_peer_complete() -> None:
    ledger = _ledger(poll_interval=0.01, poll_timeout=2.0)
    claimed = ledger.claim_side_effecting("r1", "charge", (), {}, _binding())
    _allow(ledger, claimed)

    def _complete() -> None:
        time.sleep(0.05)
        ledger.complete(
            claimed.request_id, {"paid": 1}, expected_fence=claimed.fence
        )

    threading.Thread(target=_complete, daemon=True).start()
    entry = ledger.wait_for_transition("r1")
    assert entry.result == {"paid": 1}


@pytest.mark.asyncio
async def test_wait_for_transition_async_sees_peer_complete() -> None:
    import asyncio

    ledger = _ledger(poll_interval=0.01, poll_timeout=2.0)
    claimed = ledger.claim_side_effecting("r1", "charge", (), {}, _binding())
    _allow(ledger, claimed)

    async def _complete() -> None:
        await asyncio.sleep(0.05)
        ledger.complete(
            claimed.request_id, {"paid": 2}, expected_fence=claimed.fence
        )

    task = asyncio.create_task(_complete())
    entry = await ledger.wait_for_transition_async("r1")
    await task
    assert entry.result == {"paid": 2}
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED


@pytest.mark.asyncio
async def test_wait_for_transition_async_timeout() -> None:
    ledger = _ledger(poll_interval=0.01, poll_timeout=0.05)
    ledger.claim_side_effecting("r1", "charge", (), {}, _binding())
    with pytest.raises(LedgerPollTimeoutError):
        await ledger.wait_for_transition_async("r1")
