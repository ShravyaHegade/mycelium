"""Tests for the worker-death / stream-loss signal feature.

Covers:
- LedgerEntry heartbeat/death fields and serialization round-trip
- has_worker_death_evidence() pure function
- ActionLedger with reclaim_requires_death_signal=False (constructor default)
- ActionLedger / YAML with reclaim_requires_death_signal=True (config default)
- Redis tombstone survives TTL eviction (no silent fresh claim)
- mark_worker_dead() / mark_worker_dead_for() with override_heartbeat
- release() strengthening with death-signal gate
- Claim path gating (read-only RECLAIM, side-effecting ALLOW)
- Heartbeat maintenance on claim/renew_lease
- Redis TTL floor
- CLI mark-dead + release round-trip (file backend)
"""

from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path

import pytest

from mycelium.action_ledger import (
    ActionLedger,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    LedgerPollTimeoutError,
    LedgerWorkerAliveError,
)
from mycelium.transition import (
    RetryPermission,
    SideEffectBoundary,
    SideEffectClass,
    Spendability,
    TerminalOutcome,
    ToolTransitionBinding,
    has_worker_death_evidence,
)


def _make_entry(
    *,
    request_id: str = "req-1",
    tool: str = "charge",
    status: str = "in-flight",
    terminal_outcome: str = TerminalOutcome.IN_FLIGHT.value,
    lease_until: float | None = None,
    started_at: float | None = None,
    last_heartbeat_at: float | None = None,
    worker_dead_asserted_at: float | None = None,
    worker_dead_asserted_by: str | None = None,
    side_effect_boundary: str = SideEffectBoundary.MAYBE_CROSSED.value,
    owner: str | None = None,
) -> LedgerEntry:
    return LedgerEntry(
        request_id=request_id,
        tool=tool,
        args=[],
        kwargs={},
        status=status,
        terminal_outcome=terminal_outcome,
        lease_until=lease_until,
        started_at=started_at or time.time(),
        last_heartbeat_at=last_heartbeat_at,
        worker_dead_asserted_at=worker_dead_asserted_at,
        worker_dead_asserted_by=worker_dead_asserted_by,
        side_effect_boundary=side_effect_boundary,
        owner=owner,
    )


# --- LedgerEntry field tests ---


def test_ledger_entry_heartbeat_fields_default_none() -> None:
    entry = _make_entry()
    assert entry.last_heartbeat_at is None
    assert entry.worker_dead_asserted_at is None
    assert entry.worker_dead_asserted_by is None


def test_ledger_entry_to_dict_includes_heartbeat_fields() -> None:
    entry = _make_entry(
        last_heartbeat_at=1000.0,
        worker_dead_asserted_at=2000.0,
        worker_dead_asserted_by="ops",
    )
    d = entry.to_dict()
    assert d["last_heartbeat_at"] == 1000.0
    assert d["worker_dead_asserted_at"] == 2000.0
    assert d["worker_dead_asserted_by"] == "ops"


def test_ledger_entry_from_dict_roundtrip() -> None:
    entry = _make_entry(
        last_heartbeat_at=1000.0,
        worker_dead_asserted_at=2000.0,
        worker_dead_asserted_by="ops",
    )
    d = entry.to_dict()
    restored = LedgerEntry.from_dict(d)
    assert restored.last_heartbeat_at == 1000.0
    assert restored.worker_dead_asserted_at == 2000.0
    assert restored.worker_dead_asserted_by == "ops"


# --- has_worker_death_evidence tests ---


def test_death_evidence_asserted_at_is_sufficient() -> None:
    entry = _make_entry(worker_dead_asserted_at=500.0)
    assert has_worker_death_evidence(entry, now=1000.0, presumed_dead_after=100.0) is True


def test_death_evidence_old_heartbeat_without_assertion() -> None:
    entry = _make_entry(
        started_at=100.0,
        last_heartbeat_at=200.0,
    )
    # Heartbeat at 200, now=1000, presumed_dead_after=100 → age=800 > 100
    assert has_worker_death_evidence(entry, now=1000.0, presumed_dead_after=100.0) is True


def test_death_evidence_recent_heartbeat_no_assertion() -> None:
    entry = _make_entry(
        started_at=900.0,
        last_heartbeat_at=990.0,
    )
    # Heartbeat at 990, now=1000, presumed_dead_after=100 → age=10 < 100
    assert has_worker_death_evidence(entry, now=1000.0, presumed_dead_after=100.0) is False


def test_death_evidence_no_heartbeat_uses_started_at() -> None:
    entry = _make_entry(started_at=800.0)
    # No heartbeat, fallback to started_at=800, now=1000, presumed_dead_after=100 → age=200 > 100
    assert has_worker_death_evidence(entry, now=1000.0, presumed_dead_after=100.0) is True


def test_death_evidence_assertion_overrides_recent_heartbeat() -> None:
    entry = _make_entry(
        last_heartbeat_at=999.0,
        worker_dead_asserted_at=500.0,
    )
    # Heartbeat is recent, but assertion exists → death evidence
    assert has_worker_death_evidence(entry, now=1000.0, presumed_dead_after=100.0) is True


# --- Default gate (reclaim_requires_death_signal=False) ---


def test_release_expired_without_death_signal_gate_allows_release() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(storage=storage)
    storage.set(
        _make_entry(
            lease_until=time.time() - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        )
    )
    entry = ledger_inst.release(
        "req-1", verified="not_executed", by="ops", reason="worker is dead"
    )
    assert entry.operator_resolution == "not_executed"
    assert entry.released_from_outcome == TerminalOutcome.EXPIRED.value


def test_mark_worker_dead_refuses_recent_heartbeat_without_override() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(storage=storage, presumed_dead_after=100.0)
    storage.set(_make_entry(lease_until=time.time() - 1))
    with pytest.raises(LedgerWorkerAliveError, match="worker appears alive"):
        ledger_inst.mark_worker_dead_for(
            "req-1", by="ops", reason="verified dead"
        )


def test_mark_worker_dead_override_bypasses_liveness_check() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(storage=storage, presumed_dead_after=100.0)
    storage.set(_make_entry(lease_until=time.time() - 1))
    entry = ledger_inst.mark_worker_dead_for(
        "req-1", by="ops", reason="killed the pod myself",
        override_heartbeat=True,
    )
    assert entry.worker_dead_asserted_by == "ops"
    assert entry.worker_dead_asserted_at is not None
    assert "heartbeat overridden" in (entry.resolution_reason or "")


# --- Death-signal gate ON ---


def test_release_expired_refused_without_death_evidence() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(
        storage=storage, reclaim_requires_death_signal=True
    )
    # Entry with no heartbeat, started_at is recent → worker appears alive
    storage.set(
        _make_entry(
            lease_until=time.time() - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        )
    )
    with pytest.raises(LedgerWorkerAliveError, match="worker appears alive"):
        ledger_inst.release(
            "req-1", verified="not_executed", by="ops", reason="worker is dead"
        )


def test_release_expired_allowed_with_asserted_death() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(
        storage=storage, reclaim_requires_death_signal=True
    )
    storage.set(
        _make_entry(
            lease_until=time.time() - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            worker_dead_asserted_at=time.time() - 100,
            worker_dead_asserted_by="ops",
        )
    )
    entry = ledger_inst.release(
        "req-1", verified="not_executed", by="ops", reason="worker is dead"
    )
    assert entry.operator_resolution == "not_executed"


def test_release_expired_allowed_with_old_heartbeat() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(
        storage=storage,
        reclaim_requires_death_signal=True,
        presumed_dead_after=10.0,
    )
    storage.set(
        _make_entry(
            lease_until=time.time() - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            started_at=time.time() - 1000,
            last_heartbeat_at=time.time() - 1000,
        )
    )
    entry = ledger_inst.release(
        "req-1", verified="not_executed", by="ops", reason="worker is dead"
    )
    assert entry.operator_resolution == "not_executed"


# --- mark_worker_dead / mark_worker_dead_for ---


def test_mark_worker_dead_refuses_terminal_entry() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(storage=storage)
    storage.set(
        _make_entry(
            status="completed",
            terminal_outcome=TerminalOutcome.COMPLETED.value,
        )
    )
    with pytest.raises(Exception, match="not IN_FLIGHT or EXPIRED"):
        ledger_inst.mark_worker_dead_for(
            "req-1", by="ops", reason="nope"
        )


def test_mark_worker_dead_refuses_recent_heartbeat_for_owner() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(storage=storage, presumed_dead_after=100.0)
    storage.set(_make_entry(lease_until=time.time() - 1, owner="owner-1"))
    with pytest.raises(LedgerWorkerAliveError, match="has recent heartbeat"):
        ledger_inst.mark_worker_dead(
            owner="owner-1", by="ops", reason="confirmed dead"
        )


def test_mark_worker_dead_override_bypasses_liveness_check_for_owner() -> None:
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(storage=storage, presumed_dead_after=100.0)
    storage.set(_make_entry(lease_until=time.time() - 1, owner="owner-1"))
    entries = ledger_inst.mark_worker_dead(
        owner="owner-1", by="ops", reason="killed the pod",
        override_heartbeat=True,
    )
    assert len(entries) == 1
    assert entries[0].worker_dead_asserted_by == "ops"
    assert "heartbeat overridden" in (entries[0].resolution_reason or "")


# --- Presumed dead after default ---


def test_default_presumed_dead_after_is_double_lease_ttl() -> None:
    ledger_inst = ActionLedger(lease_ttl=300)
    assert ledger_inst._presumed_dead_after == 600.0


def test_custom_presumed_dead_after() -> None:
    ledger_inst = ActionLedger(
        lease_ttl=300, presumed_dead_after=120.0
    )
    assert ledger_inst._presumed_dead_after == 120.0


# --- Claim-path gate tests ---


def _side_effect_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="test",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        spendability=Spendability.MULTI_USE,
        retry_permission=RetryPermission.SAFE_RETRY,
    )


def test_read_only_reclaim_blocked_without_death_evidence() -> None:
    """RECLAIM on read-only with gate on: recent heartbeat → poll → timeout."""
    storage = InMemoryLedgerStorage()
    now = time.time()
    storage.set(
        _make_entry(
            lease_until=now - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            started_at=now - 5,
            last_heartbeat_at=now - 2,
        )
    )
    ledger_inst = ActionLedger(
        storage=storage,
        reclaim_requires_death_signal=True,
        presumed_dead_after=100.0,
        poll_interval=0.01,
        poll_timeout=0.05,
    )
    with pytest.raises(LedgerPollTimeoutError):
        ledger_inst.claim_read_only("req-1", "search", (), {})


def test_read_only_reclaim_proceeds_with_death_evidence() -> None:
    """RECLAIM on read-only with gate on: old heartbeat → reclaim succeeds."""
    storage = InMemoryLedgerStorage()
    now = time.time()
    storage.set(
        _make_entry(
            lease_until=now - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            started_at=now - 1000,
            last_heartbeat_at=now - 1000,
        )
    )
    ledger_inst = ActionLedger(
        storage=storage,
        reclaim_requires_death_signal=True,
        presumed_dead_after=100.0,
    )
    claimed = ledger_inst.claim_read_only("req-1", "search", (), {})
    assert claimed.status == "in-flight"
    stored = storage.get("req-1")
    assert stored is not None
    assert stored.lease_until is not None
    assert stored.lease_until > now


def test_side_effecting_allow_blocked_without_death_evidence() -> None:
    """ALLOW on side-effecting with gate on: recent heartbeat → poll → hard block."""
    storage = InMemoryLedgerStorage()
    now = time.time()
    storage.set(
        _make_entry(
            lease_until=now - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
            started_at=now - 5,
            last_heartbeat_at=now - 2,
        )
    )
    ledger_inst = ActionLedger(
        storage=storage,
        reclaim_requires_death_signal=True,
        presumed_dead_after=100.0,
        poll_interval=0.01,
        poll_timeout=0.05,
    )
    with pytest.raises((LedgerHardBlockError, LedgerPollTimeoutError)):
        ledger_inst.claim_side_effecting(
            "req-1", "charge", (), {}, _side_effect_binding(),
        )


def test_side_effecting_allow_proceeds_with_death_evidence() -> None:
    """ALLOW on side-effecting with gate on: old heartbeat → reclaim succeeds."""
    storage = InMemoryLedgerStorage()
    now = time.time()
    storage.set(
        _make_entry(
            lease_until=now - 1,
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
            started_at=now - 1000,
            last_heartbeat_at=now - 1000,
        )
    )
    ledger_inst = ActionLedger(
        storage=storage,
        reclaim_requires_death_signal=True,
        presumed_dead_after=100.0,
    )
    claimed = ledger_inst.claim_side_effecting(
        "req-1", "charge", (), {}, _side_effect_binding(),
    )
    assert claimed.status == "in-flight"
    stored = storage.get("req-1")
    assert stored is not None
    assert stored.lease_until is not None
    assert stored.lease_until > now


def test_claim_maintains_heartbeat_on_renew() -> None:
    """Claim sets heartbeat; renew_lease refreshes it."""
    storage = InMemoryLedgerStorage()
    ledger_inst = ActionLedger(
        storage=storage,
        lease_ttl=60.0,
        reclaim_requires_death_signal=True,
    )
    t0 = time.time()
    entry = ledger_inst.claim("req-hb", "send_payment", (), {"amount": 10})
    assert entry.last_heartbeat_at is not None
    assert entry.last_heartbeat_at >= t0

    t1 = time.time() + 10
    renewed = ledger_inst.renew_lease(
        "req-hb", now=t1, expected_fence=entry.fence
    )
    assert renewed.last_heartbeat_at == t1
    assert renewed.lease_until is not None
    assert renewed.lease_until > t1


def test_redis_ttl_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    """Redis in_flight_ttl floored to lease_ttl * 4 when too short."""
    try:
        import fakeredis  # noqa: F401
    except ImportError:
        pytest.skip("fakeredis not installed")

    import redis as _redis

    from mycelium.storage.redis_ledger import RedisLedgerStorage

    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(_redis.Redis, "from_url", lambda url, **kw: fake)

    storage = RedisLedgerStorage(
        "redis://test",
        in_flight_ttl=100.0,  # < lease_ttl * 4 = 14400
    )
    entry = _make_entry(lease_until=time.time() + 3600)
    outcome, _ = storage.try_claim_inflight(entry, lease_ttl=3600.0)
    assert outcome == "claimed"

    ttl = fake.ttl("mycelium:action:req-1")
    assert ttl > 0
    assert ttl >= 14400


def test_redis_tombstone_blocks_fresh_claim_after_ttl_eviction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TTL-deleted in-flight key must not look like a first claim.

    Without a tombstone, Worker B would SET NX a brand-new claim and could
    double-execute. With a tombstone, get/claim rehydrate an EXPIRED ghost so
    hard-block / death-signal gates still see the prior attempt.
    """
    try:
        import fakeredis  # noqa: F401
    except ImportError:
        pytest.skip("fakeredis not installed")

    import redis as _redis

    from mycelium.storage.redis_ledger import RedisLedgerStorage

    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(_redis.Redis, "from_url", lambda url, **kw: fake)

    storage = RedisLedgerStorage("redis://test", in_flight_ttl=60.0)
    binding = _side_effect_binding()
    ledger = ActionLedger(
        storage=storage,
        reclaim_requires_death_signal=True,
        presumed_dead_after=10_000.0,
        poll_timeout=0.05,
    )
    first = ledger.claim_side_effecting(
        "req-ttl",
        "charge",
        (),
        {},
        binding,
    )
    assert first.status == "in-flight"
    assert fake.exists("mycelium:action-tomb:req-ttl")

    # Simulate Redis TTL eviction of the primary key (tombstone remains).
    fake.delete("mycelium:action:req-ttl")
    assert not fake.exists("mycelium:action:req-ttl")

    ghost = storage.get("req-ttl")
    assert ghost is not None
    assert ghost.resolved_terminal_outcome() == TerminalOutcome.EXPIRED

    body_ran = {"n": 0}

    def charge() -> dict[str, str]:
        body_ran["n"] += 1
        return {"ok": "yes"}

    # Second worker must not get a silent fresh first claim / body run.
    with pytest.raises((LedgerHardBlockError, LedgerPollTimeoutError)):
        ledger.claim_side_effecting(
            "req-ttl",
            "charge",
            (),
            {},
            binding,
        )
    assert body_ran["n"] == 0


def test_cli_mark_dead_release_round_trip_file_backend(tmp_path: Path) -> None:
    """CLI mark-dead then release on the file backend."""
    from mycelium.__main__ import main
    from mycelium.action_ledger import FileLedgerStorage

    ledger_file = tmp_path / "ledger.json"
    storage = FileLedgerStorage(ledger_file)
    ledger_inst = ActionLedger(storage=storage, reclaim_requires_death_signal=True)
    now = time.time()
    entry = ledger_inst.claim("req-cli", "send_payment", (), {"amount": 10})
    # Expire the lease and set recent heartbeat so mark-dead is refused
    expired = replace(
        entry,
        lease_until=now - 1,
        last_heartbeat_at=now - 2,
        started_at=now - 5,
    )
    storage.set(expired)

    # mark-dead without override → refused (recent heartbeat)
    assert (
        main([
            "transitions", "mark-dead", "req-cli",
            "--file", str(ledger_file),
            "--by", "ops@example.com",
            "--reason", "worker pod restarted",
        ]) == 1
    )

    # mark-dead with override → succeeds
    assert (
        main([
            "transitions", "mark-dead", "req-cli",
            "--file", str(ledger_file),
            "--by", "ops@example.com",
            "--reason", "worker pod restarted",
            "--override-heartbeat",
        ]) == 0
    )

    # release → succeeds (death evidence now present)
    assert (
        main([
            "transitions", "release", "req-cli",
            "--file", str(ledger_file),
            "--verified", "not-executed",
            "--by", "ops@example.com",
            "--reason", "worker died before effect",
        ]) == 0
    )

    released = storage.get("req-cli")
    assert released is not None
    assert released.operator_resolution == "not_executed"
    assert released.worker_dead_asserted_by == "ops@example.com"
