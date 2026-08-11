"""Tests for Redis, Postgres, and file-locked ledger storage backends."""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    LedgerPendingError,
    RedisLedgerStorage,
    SideEffectBoundary,
    SideEffectClass,
    SqliteLedgerStorage,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
    load_config_from_string,
    side_effect,
)
from mycelium.action_ledger import get_active_transition


def _entry(request_id: str, *, status: str = "in-flight") -> LedgerEntry:
    return LedgerEntry(
        request_id=request_id,
        tool="send_payment",
        args=[],
        kwargs={"amount": 10},
        status=status,
    )


def _payment_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _fake_redis(monkeypatch: pytest.MonkeyPatch):
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)

    def from_url(url: str, **kwargs: object) -> object:
        return fake

    import redis

    monkeypatch.setattr(redis.Redis, "from_url", from_url)
    return fake


def test_file_storage_serializes_concurrent_claims(tmp_path: Path) -> None:
    storage = FileLedgerStorage(tmp_path / "ledger.json")
    ledger = ActionLedger(storage=storage)
    barrier = threading.Barrier(2)
    results: list[str] = []

    def claim() -> None:
        barrier.wait()
        try:
            ledger.claim("req-1", "send_payment", (), {"amount": 10})
            results.append("claimed")
        except LedgerPendingError:
            results.append("pending")

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(results) == ["claimed", "pending"]
    assert ledger.get("req-1") is not None
    assert ledger.get("req-1").status == "in-flight"


def test_redis_storage_atomic_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_redis(monkeypatch)
    storage = RedisLedgerStorage("redis://test")
    ledger = ActionLedger(storage=storage)

    first = ledger.claim("req-redis", "send_payment", (), {"amount": 1})
    assert first.status == "in-flight"

    with pytest.raises(LedgerPendingError):
        ledger.claim("req-redis", "send_payment", (), {"amount": 1})

    completed = ledger.complete("req-redis", {"ok": True})
    assert completed.status == "completed"

    replay = ledger.claim("req-redis", "send_payment", (), {"amount": 1})
    assert replay.status == "completed"
    assert replay.result == {"ok": True}


def test_redis_storage_retries_after_failed_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_redis(monkeypatch)
    storage = RedisLedgerStorage("redis://test")
    ledger = ActionLedger(storage=storage)
    ledger.claim("req-fail", "send_payment", (), {})
    ledger.fail("req-fail", RuntimeError("boom"))

    retry = ledger.claim("req-fail", "send_payment", (), {})
    assert retry.status == "in-flight"


def test_file_storage_payment_hard_blocks_expired_lease(tmp_path: Path) -> None:
    """v1.3 transition: expired payment on file storage must hard-block."""
    storage = FileLedgerStorage(tmp_path / "ledger.json")
    ledger = ActionLedger(storage=storage)
    request_id = "file-expired-payment"

    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="send_payment",
            args=[],
            kwargs={"amount": 10.0},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
            idempotency_key=request_id,
        )
    )

    with pytest.raises(LedgerHardBlockError, match="manual reconciliation"):
        ledger.claim_side_effecting(
            request_id,
            "send_payment",
            (),
            {"amount": 10.0},
            _payment_binding(),
        )

    entry = storage.get(request_id)
    assert entry is not None
    assert entry.terminal_outcome == TerminalOutcome.BLOCKED.value


def test_file_storage_read_only_reclaims_expired_lease(tmp_path: Path) -> None:
    """v1.3 transition: expired read-only lease on file storage is reclaimable."""
    storage = FileLedgerStorage(tmp_path / "ledger.json")
    ledger = ActionLedger(storage=storage, lease_ttl=1.0, poll_interval=0.01)
    request_id = "file-expired-read"

    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="search_docs",
            args=[],
            kwargs={"query": "billing"},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
            idempotency_key=request_id,
        )
    )

    claimed = ledger.claim_read_only(
        request_id,
        "search_docs",
        (),
        {"query": "billing"},
    )
    assert claimed.status == "in-flight"
    stored = storage.get(request_id)
    assert stored is not None
    assert stored.lease_until is not None
    assert stored.lease_until > time.time()


def test_redis_storage_payment_hard_blocks_expired_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """v1.3 transition: expired payment on Redis must hard-block."""
    _fake_redis(monkeypatch)
    storage = RedisLedgerStorage("redis://test")
    ledger = ActionLedger(storage=storage)
    request_id = "redis-expired-payment"

    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="send_payment",
            args=[],
            kwargs={"amount": 10.0},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
            idempotency_key=request_id,
        )
    )

    with pytest.raises(LedgerHardBlockError, match="manual reconciliation"):
        ledger.claim_side_effecting(
            request_id,
            "send_payment",
            (),
            {"amount": 10.0},
            _payment_binding(),
        )

    entry = storage.get(request_id)
    assert entry is not None
    assert entry.terminal_outcome == TerminalOutcome.BLOCKED.value


def test_redis_storage_read_only_reclaims_expired_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """v1.3 transition: expired read-only lease on Redis is reclaimable."""
    _fake_redis(monkeypatch)
    storage = RedisLedgerStorage("redis://test")
    ledger = ActionLedger(storage=storage, lease_ttl=1.0, poll_interval=0.01)
    request_id = "redis-expired-read"

    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="search_docs",
            args=[],
            kwargs={"query": "billing"},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
            idempotency_key=request_id,
        )
    )

    claimed = ledger.claim_read_only(
        request_id,
        "search_docs",
        (),
        {"query": "billing"},
    )
    assert claimed.status == "in-flight"
    stored = storage.get(request_id)
    assert stored is not None
    assert stored.lease_until is not None
    assert stored.lease_until > time.time()


def test_redis_storage_read_only_returns_completed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """v1.3 transition: completed read-only result is returned from Redis."""
    _fake_redis(monkeypatch)
    storage = RedisLedgerStorage("redis://test")
    ledger = ActionLedger(storage=storage)
    request_id = "redis-completed-read"

    claimed = ledger.claim_read_only(
        request_id,
        "search_docs",
        (),
        {"query": "billing"},
    )
    assert claimed.status == "in-flight"
    ledger.complete(request_id, {"query": "billing", "hits": 1})

    replay = ledger.claim_read_only(
        request_id,
        "search_docs",
        (),
        {"query": "billing"},
    )
    assert replay.status == "completed"
    assert replay.result == {"query": "billing", "hits": 1}


def test_postgres_storage_atomic_claim() -> None:
    from backend_gates import require_postgres_dsn_or_skip

    from mycelium import PostgresLedgerStorage

    dsn = require_postgres_dsn_or_skip()
    storage = PostgresLedgerStorage(dsn, table="mycelium_test_action_ledger")
    ledger = ActionLedger(storage=storage)

    # Unique per invocation so re-running the suite (or CI's focused concurrency
    # step after the full suite) does not collide with a prior COMPLETED row.
    request_id = f"req-pg-integration-{uuid.uuid4().hex}"

    first = ledger.claim(request_id, "send_payment", (), {"amount": 99})
    assert first.status == "in-flight"

    with pytest.raises(LedgerPendingError):
        ledger.claim(request_id, "send_payment", (), {"amount": 99})

    ledger.complete(request_id, {"paid": True})
    replay = ledger.claim(request_id, "send_payment", (), {"amount": 99})
    assert replay.status == "completed"
    assert replay.result == {"paid": True}


def test_config_builds_redis_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("fakeredis")
    from mycelium.config import MyceliumConfig

    monkeypatch.setenv("MYCELIUM_REDIS_URL", "redis://localhost:6379/0")
    storage = MyceliumConfig._build_ledger_storage(
        {
            "storage": "redis",
            "url_env": "MYCELIUM_REDIS_URL",
            "prefix": "test:action:",
        }
    )
    assert isinstance(storage, RedisLedgerStorage)


def test_config_builds_postgres_storage() -> None:
    pytest.importorskip("psycopg")
    from mycelium.config import MyceliumConfig
    from mycelium.storage.postgres_ledger import PostgresLedgerStorage

    storage = MyceliumConfig._build_ledger_storage(
        {
            "storage": "postgres",
            "dsn": "postgresql://example",
            "table": "mycelium_action_ledger",
        }
    )
    assert isinstance(storage, PostgresLedgerStorage)


def test_sqlite_storage_atomic_claim(tmp_path: Path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.db")
    ledger = ActionLedger(storage=storage)

    first = ledger.claim("req-sqlite", "send_payment", (), {"amount": 1})
    assert first.status == "in-flight"

    with pytest.raises(LedgerPendingError):
        ledger.claim("req-sqlite", "send_payment", (), {"amount": 1})

    completed = ledger.complete("req-sqlite", {"ok": True})
    assert completed.status == "completed"

    replay = ledger.claim("req-sqlite", "send_payment", (), {"amount": 1})
    assert replay.status == "completed"
    assert replay.result == {"ok": True}


def test_sqlite_storage_serializes_concurrent_claims(tmp_path: Path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.db")
    ledger = ActionLedger(storage=storage)
    barrier = threading.Barrier(2)
    results: list[str] = []

    def claim() -> None:
        barrier.wait()
        try:
            ledger.claim("req-1", "send_payment", (), {"amount": 10})
            results.append("claimed")
        except LedgerPendingError:
            results.append("pending")

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(results) == ["claimed", "pending"]
    assert ledger.get("req-1") is not None
    assert ledger.get("req-1").status == "in-flight"


def test_sqlite_storage_payment_hard_blocks_expired_lease(tmp_path: Path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.db")
    ledger = ActionLedger(storage=storage)
    request_id = "sqlite-expired-payment"

    storage.set(
        LedgerEntry(
            request_id=request_id,
            tool="send_payment",
            args=[],
            kwargs={"amount": 10.0},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
            idempotency_key=request_id,
        )
    )

    with pytest.raises(LedgerHardBlockError, match="manual reconciliation"):
        ledger.claim_side_effecting(
            request_id,
            "send_payment",
            (),
            {"amount": 10.0},
            _payment_binding(),
        )

    entry = storage.get(request_id)
    assert entry is not None
    assert entry.terminal_outcome == TerminalOutcome.BLOCKED.value


def test_sqlite_cas_transition_owner_fence(tmp_path: Path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.db")
    entry = LedgerEntry(
        request_id="cas-1",
        tool="send_payment",
        args=[],
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        owner="owner-a",
        idempotency_key="cas-1",
    )
    storage.set(entry)
    completed = LedgerEntry(
        request_id="cas-1",
        tool="send_payment",
        args=[],
        kwargs={},
        status="completed",
        terminal_outcome=TerminalOutcome.COMPLETED.value,
        result={"ok": True},
        owner="owner-a",
        idempotency_key="cas-1",
    )
    assert storage.try_transition(
        completed,
        expected_terminal_outcomes=frozenset({TerminalOutcome.IN_FLIGHT.value}),
        expected_owner="owner-a",
    )
    assert not storage.try_transition(
        completed,
        expected_terminal_outcomes=frozenset({TerminalOutcome.IN_FLIGHT.value}),
        expected_owner="owner-b",
    )
    stored = storage.get("cas-1")
    assert stored is not None
    assert stored.status == "completed"


def test_config_builds_sqlite_storage(tmp_path: Path) -> None:
    from mycelium.config import MyceliumConfig

    storage = MyceliumConfig._build_ledger_storage(
        {
            "storage": "sqlite",
            "path": str(tmp_path / "ledger.db"),
            "table": "mycelium_action_ledger",
        }
    )
    assert isinstance(storage, SqliteLedgerStorage)


def test_config_sqlite_requires_path() -> None:
    from mycelium.config import ConfigError, MyceliumConfig

    with pytest.raises(ConfigError, match="requires a 'path'"):
        MyceliumConfig._build_ledger_storage({"storage": "sqlite"})


def test_sqlite_yaml_loads_without_optional_dependencies(tmp_path: Path) -> None:
    """SQLite is stdlib — YAML load + storage build must not import redis/psycopg."""
    from mycelium.config import MyceliumConfig

    yaml_text = f"""\
transition:
  agent_id: demo
  policy_version: "1"
  scope_from: {{}}
  reclaim_requires_death_signal: true
action_ledger:
  storage: sqlite
  path: {tmp_path / "mycelium-ledger.db"}
  unclassified_policy: strict
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    config = load_config_from_string(yaml_text)
    assert config.action_ledger is not None
    assert config.action_ledger["storage"] == "sqlite"
    ledger_cfg = config.tools["charge"].ledger
    assert ledger_cfg is not None
    storage = MyceliumConfig._build_ledger_storage(ledger_cfg)
    assert isinstance(storage, SqliteLedgerStorage)


class _ProcessCrash(BaseException):
    """Simulates process death before the ledger can record failure."""


def test_sqlite_completed_result_survives_new_ledger_instance(tmp_path: Path) -> None:
    """A new ActionLedger on the same SQLite file returns the stored result."""
    db = tmp_path / "ledger.db"
    executions: list[str] = []

    storage1 = SqliteLedgerStorage(db)

    @ledger_sync(storage=storage1, transition_binding=_payment_binding())
    def charge(amount: float) -> dict[str, bool]:
        executions.append("first")
        return {"charged": True}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        first = charge(amount=10.0, tool_call_id="c1")

    storage2 = SqliteLedgerStorage(db)

    @ledger_sync(storage=storage2, transition_binding=_payment_binding())
    def charge(amount: float) -> dict[str, bool]:  # noqa: F811
        executions.append("second")
        return {"charged": True}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        replay = charge(amount=10.0, tool_call_id="c1")

    assert first == replay == {"charged": True}
    assert executions == ["first"]


def test_sqlite_maybe_crossed_survives_restart_and_does_not_reexecute(
    tmp_path: Path,
) -> None:
    """Crash inside side_effect() must persist maybe_crossed and block redispatch.

    A new ledger instance on the same SQLite file must not run the body again.
    Proves persistence *and* non-reexecution, not merely that SQLite wrote a row.
    """
    db = tmp_path / "ledger.db"
    executions: list[str] = []
    request_ids: list[str] = []
    lease_ttl = 0.05

    storage1 = SqliteLedgerStorage(db)

    @ledger_sync(
        storage=storage1,
        transition_binding=_payment_binding(),
        lease_ttl=lease_ttl,
        lease_renew_interval=0,
        poll_interval=0.01,
        poll_timeout=0.2,
        reclaim_requires_death_signal=True,
    )
    def charge(amount: float) -> dict[str, bool]:
        executions.append("first")
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        with side_effect():
            raise _ProcessCrash("killed inside side_effect()")

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        with pytest.raises(_ProcessCrash):
            charge(amount=10.0, tool_call_id="c1")

    assert request_ids, "first instance never claimed a transition"
    crashed = storage1.get(request_ids[0])
    assert crashed is not None, "crash window entry was not written to SQLite"
    assert crashed.side_effect_boundary == SideEffectBoundary.MAYBE_CROSSED.value
    assert crashed.terminal_outcome == TerminalOutcome.IN_FLIGHT.value

    # Process is gone: lease is no longer renewed. Persist expiry on the
    # same row so the next instance sees EXPIRED + maybe_crossed.
    storage1.set(replace(crashed, lease_until=time.time() - 1))

    storage2 = SqliteLedgerStorage(db)
    restarted = storage2.get(crashed.request_id)
    assert restarted is not None
    assert restarted.side_effect_boundary == SideEffectBoundary.MAYBE_CROSSED.value
    assert restarted.request_id == crashed.request_id
    assert restarted.resolved_terminal_outcome() == TerminalOutcome.EXPIRED

    @ledger_sync(
        storage=storage2,
        transition_binding=_payment_binding(),
        lease_ttl=lease_ttl,
        lease_renew_interval=0,
        poll_interval=0.01,
        poll_timeout=0.2,
        reclaim_requires_death_signal=True,
    )
    def charge(amount: float) -> dict[str, bool]:  # noqa: F811
        executions.append("second")
        with side_effect():
            return {"charged": True}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        with pytest.raises(LedgerHardBlockError):
            charge(amount=10.0, tool_call_id="c1")

    assert executions == ["first"]
    blocked = storage2.get(crashed.request_id)
    assert blocked is not None
    assert blocked.side_effect_boundary == SideEffectBoundary.MAYBE_CROSSED.value
