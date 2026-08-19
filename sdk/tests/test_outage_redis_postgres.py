"""Backend outage tests (Phase 4 / outage): Redis and Postgres storage down.

Mirrors the fail-closed contract in ``test_fail_closed_storage.py`` but
exercised against the real Redis and Postgres storage backends:

- claim during outage        -> ``LedgerStorageUnavailableError``; tool never runs
- complete during outage     -> ``LedgerStorageUnavailableError``; entry stays
  IN_FLIGHT (no phantom COMPLETED)
- failure-recording during outage -> original tool exception surfaces unmasked
- mid-reconcile outage       -> fail-closed: no re-execution, no phantom
  COMPLETED, the ``LedgerStorageUnavailableError`` propagates

Determinism / no external deps: Redis runs on ``fakeredis`` (exercises the real
``RedisEntryStorage`` claim/transition code paths). Postgres is exercised with a
broken client injected at the ``_require_psycopg`` boundary, so the suite needs
neither a real Redis nor a real Postgres (psycopg is not installed by default).
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from mycelium import (
    ActionLedger,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerStorageUnavailableError,
    ReconcileResult,
    RedisLedgerStorage,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="outage",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _scope() -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id="r1")


class _StubReconciler:
    def __init__(self, result: ReconcileResult) -> None:
        self._result = result
        self.calls = 0

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        self.calls += 1
        return self._result


def _seed_ambiguous_payment(
    storage: object,
    tool_name: str,
    *,
    op_ref: str,
    request_id: str | None = None,
) -> str:
    """Persist an expired, maybe-crossed, IN_FLIGHT entry for *tool_name* so a
    redispatch under ``_scope()`` resolves HARD_BLOCK and reconciles.

    Returns the request_id that a ``@ledger_sync`` dispatch derives (seeded
    entries must use the same derived key to be seen by the gate).
    """
    # Match decorator call shape: send_payment(10.0) → args=(10.0,), kwargs={}
    with execution_scope(_scope()):
        derived = ActionLedger(
            storage=InMemoryLedgerStorage()
        ).derive_request_id(
            tool_name, (10.0,), {}, transition_binding=_binding()
        )
    rid = request_id or derived
    storage.set(
        LedgerEntry(
            request_id=rid,
            tool=tool_name,
            args=[10.0],
            kwargs={},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
            side_effect_boundary=SideEffectBoundary.MAYBE_CROSSED.value,
            external_operation_ref=op_ref,
        )
    )
    return rid


# ---------------------------------------------------------------------------
# Redis outage
# ---------------------------------------------------------------------------


class FailingRedisStorage(RedisLedgerStorage):
    """Real Redis storage (fakeredis-backed) with per-operation outage toggles."""

    def __init__(self, url: str, *, prefix: str = "mycelium:outage:") -> None:
        super().__init__(url, prefix=prefix)
        self.fail_get = False
        self.fail_set = False
        self.fail_claim = False
        self.fail_transition = False
        self.fail_list_all = False

    def get(self, request_id: str) -> LedgerEntry | None:
        if self.fail_get:
            raise ConnectionError("storage backend unreachable")
        return super().get(request_id)

    def set(self, entry: LedgerEntry) -> None:
        if self.fail_set:
            raise ConnectionError("storage backend unreachable")
        super().set(entry)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[str, LedgerEntry | None]:
        if self.fail_claim:
            raise ConnectionError("storage backend unreachable")
        return super().try_claim_inflight(entry, lease_ttl=lease_ttl)

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
    ) -> bool:
        if self.fail_transition:
            raise ConnectionError("storage backend unreachable")
        return super().try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
            expected_fence=expected_fence,
        )

    def list_all(self) -> list[LedgerEntry]:
        if self.fail_list_all:
            raise ConnectionError("storage backend unreachable")
        return super().list_all()


class TestRedisOutage:
    def _storage(self, monkeypatch: pytest.MonkeyPatch) -> FailingRedisStorage:
        fakeredis = pytest.importorskip("fakeredis")
        fake = fakeredis.FakeRedis(decode_responses=True)
        import redis

        monkeypatch.setattr(redis.Redis, "from_url", lambda url, **kw: fake)
        return FailingRedisStorage("redis://outage", prefix="mycelium:outage:")

    def test_claim_during_outage_raises_storage_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        storage = self._storage(monkeypatch)
        ledger_inst = ActionLedger(storage=storage)
        storage.fail_claim = True
        with pytest.raises(LedgerStorageUnavailableError, match="try_claim_inflight"):
            ledger_inst.claim_side_effecting(
                "req-redis-claim", "send_payment", (), {"amount": 10}, _binding()
            )

    def test_tool_never_runs_on_storage_down_claim(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        storage = self._storage(monkeypatch)
        storage.fail_claim = True
        called = False

        @ledger_sync(storage=storage, transition_binding=_binding())
        def send_payment(amount: float) -> dict[str, bool]:
            nonlocal called
            called = True
            return {"ok": True}

        with execution_scope(_scope()):
            with pytest.raises(LedgerStorageUnavailableError):
                send_payment(10.0)
        assert not called

    def test_complete_during_outage_keeps_inflight(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        storage = self._storage(monkeypatch)
        ledger_inst = ActionLedger(storage=storage)
        # Claim succeeds while the backend is up, then it goes down mid-flight.
        ledger_inst.claim("req-redis-complete", "send_payment", (), {"amount": 10})
        storage.fail_transition = True
        with pytest.raises(LedgerStorageUnavailableError, match="try_transition"):
            ledger_inst.complete("req-redis-complete", {"ok": True})
        storage.fail_transition = False
        stored = storage.get("req-redis-complete")
        assert stored is not None
        assert stored.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT

    def test_failure_recording_outage_surfaces_original_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Storage down while recording a tool failure must not mask the tool's
        own exception (it propagates, not the storage error)."""
        storage = self._storage(monkeypatch)
        storage.fail_transition = True

        @ledger_sync(storage=storage, transition_binding=_binding())
        def failing_tool(amount: float) -> dict[str, bool]:
            raise ValueError("tool broke")

        with execution_scope(_scope()):
            with pytest.raises(ValueError, match="tool broke"):
                failing_tool(10.0)

    def test_mid_reconcile_storage_outage_fail_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The reconciler says COMPLETED but the storage write of that result
        fails: must raise LedgerStorageUnavailableError, not return a result,
        and must NOT mark the entry COMPLETED (fail-closed, no phantom)."""
        storage = self._storage(monkeypatch)
        reconciler = _StubReconciler(
            ReconcileResult.completed({"charged": True, "id": "pi_x"})
        )
        # Seed an expired, ambiguous transition so the redispatch gate is
        # HARD_BLOCK -> reconcile, not a fresh claim that just runs the tool.
        rid = _seed_ambiguous_payment(storage, "send_payment", op_ref="pi_x")

        storage.fail_transition = True

        @ledger_sync(storage=storage, transition_binding=_binding(), reconciler=reconciler)
        def send_payment(amount: float) -> dict[str, bool]:
            raise AssertionError("tool must not re-execute during outage")

        with execution_scope(_scope()):
            with pytest.raises(LedgerStorageUnavailableError, match="try_transition"):
                send_payment(10.0)

        assert reconciler.calls == 1, "reconciler should have been consulted"
        # The reconcile write failed so the entry must NOT be a phantom
        # COMPLETED: the durable stored outcome is still the pre-reconcile
        # IN_FLIGHT (read-time resolution shows EXPIRED via the lapsed lease).
        storage.fail_transition = False
        stored = storage.get(rid)
        assert stored is not None
        assert stored.terminal_outcome == TerminalOutcome.IN_FLIGHT.value
        assert not stored.is_terminal_completed()


# ---------------------------------------------------------------------------
# Postgres outage
# ---------------------------------------------------------------------------


class _StubSQL:
    """Minimal stand-in for ``psycopg.sql`` used by the storage module."""

    class _Compiled:
        def __init__(self, text: str) -> None:
            self._text = text

        def format(self, *_args: object) -> str:
            return self._text

    @staticmethod
    def SQL(text: str) -> _StubSQL._Compiled:
        return _StubSQL._Compiled(text)

    @staticmethod
    def Identifier(name: str) -> str:
        return name


class _DownPsycopg:
    """Fake ``psycopg`` module whose ``connect`` models a Postgres outage."""

    @staticmethod
    def connect(dsn: str) -> None:
        raise ConnectionError("postgres down")


class FailingPostgresStorage:
    """Postgres-shaped storage whose operations fail on demand.

    Construction exercises the real ``PostgresLedgerStorage.__init__`` path
    (against a stubbed ``_require_psycopg``), while each operation either
    delegates to an in-memory backing store or raises ``ConnectionError`` to
    simulate a backend outage. This keeps the fail-closed contract tests
    deterministic without a live Postgres.
    """

    def __init__(self) -> None:
        from mycelium.storage.postgres_ledger import PostgresLedgerStorage

        self._inner = PostgresLedgerStorage("postgresql://outage:5432/mycelium")
        self._mem = InMemoryLedgerStorage()
        self.fail_get = False
        self.fail_set = False
        self.fail_claim = False
        self.fail_transition = False
        self.fail_list_all = False

    def get(self, request_id: str) -> LedgerEntry | None:
        if self.fail_get:
            raise ConnectionError("storage backend unreachable")
        return self._mem.get(request_id)

    def set(self, entry: LedgerEntry) -> None:
        if self.fail_set:
            raise ConnectionError("storage backend unreachable")
        self._mem.set(entry)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[str, LedgerEntry | None]:
        if self.fail_claim:
            raise ConnectionError("storage backend unreachable")
        return self._mem.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
    ) -> bool:
        if self.fail_transition:
            raise ConnectionError("storage backend unreachable")
        return self._mem.try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
            expected_fence=expected_fence,
        )

    def list_all(self) -> list[LedgerEntry]:
        if self.fail_list_all:
            raise ConnectionError("storage backend unreachable")
        return self._mem.list_all()


@pytest.fixture
def stub_psycopg(monkeypatch: pytest.MonkeyPatch) -> None:
    """Route ``_require_psycopg`` to the outage stub so Postgres storage can be
    constructed without the (uninstalled) psycopg package."""
    import mycelium.storage.postgres_ledger as pg_mod

    monkeypatch.setattr(
        pg_mod, "_require_psycopg", lambda: (_DownPsycopg(), _StubSQL())
    )


class TestPostgresOutage:
    def test_claim_during_outage_raises_storage_unavailable(
        self, stub_psycopg: None
    ) -> None:
        from mycelium import PostgresLedgerStorage

        storage = PostgresLedgerStorage("postgresql://outage:5432/mycelium")
        ledger_inst = ActionLedger(storage=storage)
        with pytest.raises(
            LedgerStorageUnavailableError, match=r"during (get|try_claim_inflight)"
        ):
            ledger_inst.claim_side_effecting(
                "req-pg-claim", "send_payment", (), {"amount": 10}, _binding()
            )

    def test_tool_never_runs_on_storage_down_claim(self, stub_psycopg: None) -> None:
        storage = FailingPostgresStorage()
        storage.fail_claim = True
        called = False

        @ledger_sync(storage=storage, transition_binding=_binding())
        def send_payment(amount: float) -> dict[str, bool]:
            nonlocal called
            called = True
            return {"ok": True}

        with execution_scope(_scope()):
            with pytest.raises(LedgerStorageUnavailableError):
                send_payment(10.0)
        assert not called

    def test_complete_during_outage_keeps_inflight(self, stub_psycopg: None) -> None:
        storage = FailingPostgresStorage()
        ledger_inst = ActionLedger(storage=storage)
        ledger_inst.claim("req-pg-complete", "send_payment", (), {"amount": 10})
        storage.fail_transition = True
        with pytest.raises(LedgerStorageUnavailableError, match="try_transition"):
            ledger_inst.complete("req-pg-complete", {"ok": True})
        storage.fail_transition = False
        stored = storage.get("req-pg-complete")
        assert stored is not None
        assert stored.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT

    def test_failure_recording_outage_surfaces_original_exception(
        self, stub_psycopg: None
    ) -> None:
        storage = FailingPostgresStorage()
        storage.fail_transition = True

        @ledger_sync(storage=storage, transition_binding=_binding())
        def failing_tool(amount: float) -> dict[str, bool]:
            raise ValueError("tool broke")

        with execution_scope(_scope()):
            with pytest.raises(ValueError, match="tool broke"):
                failing_tool(10.0)

    def test_mid_reconcile_storage_outage_fail_closed(
        self, stub_psycopg: None
    ) -> None:
        storage = FailingPostgresStorage()
        reconciler = _StubReconciler(
            ReconcileResult.completed({"charged": True, "id": "pi_x"})
        )
        rid = _seed_ambiguous_payment(storage, "send_payment", op_ref="pi_x")

        storage.fail_transition = True

        @ledger_sync(storage=storage, transition_binding=_binding(), reconciler=reconciler)
        def send_payment(amount: float) -> dict[str, bool]:
            raise AssertionError("tool must not re-execute during outage")

        with execution_scope(_scope()):
            with pytest.raises(LedgerStorageUnavailableError, match="try_transition"):
                send_payment(10.0)

        assert reconciler.calls == 1
        storage.fail_transition = False
        stored = storage.get(rid)
        assert stored is not None
        assert stored.terminal_outcome == TerminalOutcome.IN_FLIGHT.value
        assert not stored.is_terminal_completed()


# ---------------------------------------------------------------------------
# Old entries deserialize against a broken client (real Redis code path)
# ---------------------------------------------------------------------------


class TestRedisTransportFailure:
    def test_real_redis_entry_path_wraps_connection_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ConnectionError raised by the Redis client (real storage code path)
        is wrapped as LedgerStorageUnavailableError, never leaked raw."""
        fakeredis = pytest.importorskip("fakeredis")
        fake = fakeredis.FakeRedis(decode_responses=True)
        fake.execute_command = MagicMock(side_effect=ConnectionError("redis down"))

        import redis

        monkeypatch.setattr(redis.Redis, "from_url", lambda url, **kw: fake)

        storage = RedisLedgerStorage("redis://outage", prefix="mycelium:transport:")
        ledger_inst = ActionLedger(storage=storage)
        with pytest.raises(LedgerStorageUnavailableError):
            ledger_inst.claim("req-transport", "send_payment", (), {})
