"""Regression tests for the three probes from Mengchheang's public repro.

Source: notes/design-partners/mycelium_1_16_0_public_repro.py
Port: three probes ported into pytest.

Scope (same as original):
- In-memory / fakeredis only; not a real-Redis certification.
- Public API only (no internal implementation traps).
- No external side effects.

Pre-1.18.1 baselines (buggy — documented here for historical reference):

    Probe 1 — Semantic identity (same caller request_id, changed args):
        expected_1_16_0 = {"keys_equal": false, "executions": [10, 11]}
        post-1.18.1 assertion: unchanged — intentional design, not a bug.

    Probe 2 — Concurrent NOT_EXECUTED reconcile:
        expected_1_16_0 = {"executions": 2, "results": [30, 30], "errors": []}
        post-1.18.1 assertion: executions == 1 (CAS loser polls winner).

    Probe 3 — Concurrent expired Redis reclaim:
        expected_1_16_0 = {"claim_results": ["claimed", "claimed"]}
        post-1.18.1 assertion: at most one "claimed"; other is non-claiming.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from mycelium import (
    ARGS_DRIFT_OFF,
    InMemoryLedgerStorage,
    LedgerEntry,
    ReconcileResult,
    RedisLedgerStorage,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolBoundaryError,
    ToolTransitionBinding,
    derive_transition_key_for_call,
    ledger_sync,
)

# ---------------------------------------------------------------------------
# Probe 1 — Semantic identity
# ---------------------------------------------------------------------------
# derive_transition_key_for_call still hashes args, so the same caller
# request_id with different args yields different *hash* keys. The decorator
# now treats an explicit request_id as the storage identity, so a retry with
# changed args is fail-closed (args-drift) instead of a second transition.

_BINDING = ToolTransitionBinding.for_tool(
    agent_id="public-repro",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)


def test_semantic_identity() -> None:
    """Hash keys still split on args; explicit request_id does not re-execute."""
    storage = InMemoryLedgerStorage()
    executions: list[int] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        on_args_drift=ARGS_DRIFT_OFF,
    )
    def charge(amount: int) -> int:
        executions.append(amount)
        return amount

    kwargs_a = {"amount": 10, "request_id": "intent-1"}
    kwargs_b = {"amount": 11, "request_id": "intent-1"}
    key_a = derive_transition_key_for_call("charge", (), kwargs_a, _BINDING)
    key_b = derive_transition_key_for_call("charge", (), kwargs_b, _BINDING)
    charge(**kwargs_a)
    with pytest.raises(ToolBoundaryError, match="identity conflict"):
        charge(**kwargs_b)

    assert key_a != key_b, "changed args should produce different transition key"
    assert executions == [10]


# ---------------------------------------------------------------------------
# Probe 2 — Concurrent NOT_EXECUTED reconcile
# ---------------------------------------------------------------------------
# Two threads both reconcile UNKNOWN → NOT_EXECUTED at the same time.
# Pre-1.18.1 both claimed IN_FLIGHT and both executed the tool.
# Post-1.18.1 the CAS loser polls the winner's result.

class BarrierReconciler:
    """Holds both threads at a barrier before returning NOT_EXECUTED."""

    def __init__(self) -> None:
        self.barrier = threading.Barrier(2, timeout=5)
        self.calls = 0
        self.lock = threading.Lock()

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        with self.lock:
            self.calls += 1
        self.barrier.wait()
        return ReconcileResult.not_executed()


def test_concurrent_reconcile_not_executed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two threads race on reconcile NOT_EXECUTED: at most one executes."""
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)
    import redis as redis_mod
    monkeypatch.setattr(redis_mod.Redis, "from_url", lambda url, **kw: fake)
    storage = RedisLedgerStorage("redis://test")

    reconciler = BarrierReconciler()
    executions: list[int] = []
    execution_lock = threading.Lock()

    @ledger_sync(
        storage=storage,
        transition_binding=_BINDING,
        reconciler=reconciler,
        poll_interval=0.001,
        poll_timeout=2,
    )
    def charge(amount: int) -> int:
        with execution_lock:
            executions.append(amount)
        time.sleep(0.02)
        return amount

    kwargs = {"amount": 30, "request_id": "reconcile-race"}
    transition_key = derive_transition_key_for_call(
        "charge", (), kwargs, _BINDING
    )
    storage.set(
        LedgerEntry(
            request_id=transition_key,
            tool="charge",
            args=[],
            kwargs={"amount": 30},
            status="failed",
            terminal_outcome=TerminalOutcome.UNKNOWN.value,
            side_effect_boundary=SideEffectBoundary.MAYBE_CROSSED.value,
            external_operation_ref="provider:op-30",
        )
    )

    start = threading.Barrier(2, timeout=5)
    results: list[int] = []
    errors: list[str] = []

    def run() -> None:
        try:
            start.wait()
            results.append(charge(**kwargs))
        except BaseException as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    # Post-1.18.1 assertion: at most one execution
    assert len(executions) == 1, (
        f"expected exactly 1 tool execution, got {len(executions)}"
    )
    assert errors == [], f"unexpected errors: {errors}"
    assert len(results) == 2, (
        f"both callers should get a result, got {results}"
    )
    # Both callers see the same completed result (30)
    assert sorted(results) == [30, 30], (
        f"both callers should see 30, got {results}"
    )


# ---------------------------------------------------------------------------
# Probe 3 — Concurrent expired Redis reclaim
# ---------------------------------------------------------------------------
# Two threads both reclaim the same lease-expired in-flight entry.
# Pre-1.18.1: both got "claimed" (bare SET in the stale branch).
# Post-1.18.1: _try_reclaim uses WATCH/MULTI; exactly one "claimed".


class BarrierClient:
    """Force two Redis-adapter readers to see the same stale value first."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.read_barrier = threading.Barrier(2, timeout=5)

    def get(self, key: str) -> Any:
        value = self.inner.get(key)
        self.read_barrier.wait()
        return value

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


def test_concurrent_expired_redis_reclaim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two threads reclaim an expired IN_FLIGHT: exactly one 'claimed'."""
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)
    import redis as redis_mod
    monkeypatch.setattr(redis_mod.Redis, "from_url", lambda url, **kw: fake)

    storage = RedisLedgerStorage("redis://test")
    request_id = "redis-stale-race"
    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="search_docs",
            args=[],
            kwargs={"query": "x"},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
        )
    )

    storage._inner._client = BarrierClient(fake)

    claim_entry = LedgerEntry(
        request_id=request_id,
        tool="search_docs",
        args=[],
        kwargs={"query": "x"},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
    )
    outcomes: list[str] = []
    errors: list[str] = []

    def reclaim() -> None:
        try:
            outcome, _ = storage.try_claim_inflight(claim_entry, lease_ttl=60)
            outcomes.append(outcome)
        except BaseException as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=reclaim) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    claimed = [o for o in outcomes if o == "claimed"]
    non_claimed = [o for o in outcomes if o != "claimed"]
    assert errors == [], f"unexpected errors: {errors}"
    assert len(claimed) == 1, (
        f"expected exactly 1 'claimed', got {len(claimed)}: {outcomes}"
    )
    assert len(non_claimed) == 1 and non_claimed[0] in (
        "in_flight",
        "completed",
    ), (
        f"expected non-claiming outcome, got {non_claimed}"
    )
