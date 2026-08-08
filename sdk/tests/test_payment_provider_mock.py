"""Stripe-shaped payment provider mock (Phase 4 / provider reconciler).

A read-only, Stripe-shaped ``Reconciler`` drives a fake PaymentIntent store.
The store mirrors the real PaymentIntent lifecycle
(``requires_payment_method`` -> ``processing`` -> ``succeeded``, or
``canceled``), and the reconciler classifies each state fail-closed:

- ``succeeded``           -> COMPLETED   (money moved; never re-run)
- ``requires_payment_method`` / ``canceled`` / missing -> NOT_EXECUTED
  (provably no money moved; exactly one re-execution is granted)
- ``processing``          -> UNKNOWN     (outcome ambiguous; HARD_BLOCK,
  no re-execution)

Guarantees asserted across crash/retry scenarios:

- a charge happens at most once per transition key — never more
- the reconciler is strictly read-only (never creates or charges)
- post-COMPLETED redispatches return the cached result without re-charging
- the HARD_BLOCK path never re-executes, and an operator NOT_EXECUTED release
  grants exactly one more charge
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import fakeredis
import pytest
import redis

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    LedgerReleaseRefusedError,
    ReconcileResult,
    RedisLedgerStorage,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    get_ledger,
    ledger_sync,
    record_external_operation,
    side_effect,
)


class _FakePaymentProvider:
    """In-memory Stripe-shaped PaymentIntent store.

    ``charges`` is the money-moved log: every successful ``charge()`` appends
    the intent id.  The reconciler may only read via ``retrieve()``.
    """

    def __init__(self) -> None:
        self._intents: dict[str, dict[str, object]] = {}
        self._counter = 0
        self.charges: list[str] = []
        self.retrieve_calls = 0

    def create(self, amount: float) -> str:
        self._counter += 1
        pid = f"pi_mock_{self._counter}"
        self._intents[pid] = {
            "id": pid,
            "amount": amount,
            "status": "requires_payment_method",
        }
        return pid

    def retrieve(self, pid: str) -> dict[str, object] | None:
        self.retrieve_calls += 1
        intent = self._intents.get(pid)
        return dict(intent) if intent is not None else None

    def charge(self, pid: str) -> None:
        intent = self._intents.get(pid)
        if intent is None:
            raise RuntimeError(f"unknown payment intent {pid!r}")
        intent["status"] = "succeeded"
        self.charges.append(pid)

    def set_status(self, pid: str, status: str) -> None:
        self._intents[pid]["status"] = status

    def snapshot(self) -> str:
        return json.dumps(self._intents, sort_keys=True)


class _StripeReconciler:
    """Read-only provider reconciler. Never mutates provider state."""

    def __init__(self, provider: _FakePaymentProvider) -> None:
        self._provider = provider

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        pid = entry.external_operation_ref
        if not pid:
            return ReconcileResult.unknown()
        intent = self._provider.retrieve(pid)
        if intent is None:
            return ReconcileResult.not_executed()
        status = intent["status"]
        if status == "succeeded":
            return ReconcileResult.completed({"charged": True, "payment_intent": pid})
        if status == "processing":
            return ReconcileResult.unknown()
        return ReconcileResult.not_executed()


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="payments",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _scope() -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id="r1")


def _make_charge(provider, storage, reconciler=None):
    @ledger_sync(
        storage=storage,
        transition_binding=_binding(),
        reconciler=reconciler,
    )
    def charge(amount: float) -> dict:
        pid = provider.create(amount)
        record_external_operation(pid)
        with side_effect():
            provider.charge(pid)
        return {"charged": True, "payment_intent": pid}

    return charge


def _request_id(ledger_inst: ActionLedger, kwargs: dict) -> str:
    with execution_scope(_scope()):
        return ledger_inst.derive_request_id(
            "charge", (), kwargs, transition_binding=_binding()
        )


@pytest.fixture(params=["file", "redis"])
def storage(request, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    if request.param == "file":
        return FileLedgerStorage(tmp_path / "ledger.json")
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis.Redis, "from_url", lambda url, **kwargs: fake)
    return RedisLedgerStorage("redis://test")


def _seed_crashed_charge(
    provider: _FakePaymentProvider,
    storage,
    *,
    status: str,
) -> tuple[str, str]:
    """Persist an expired, maybe-crossed IN_FLIGHT entry whose provider intent
    sits in *status* — the durable leftovers of a worker that died mid-charge.

    Returns ``(request_id, payment_intent_id)``.
    """
    pid = provider.create(10.0)
    if status != "requires_payment_method":
        provider.set_status(pid, status)
    with execution_scope(_scope()):
        derived = ActionLedger(storage=InMemoryLedgerStorage()).derive_request_id(
            "charge", (), {"amount": 10.0, "tool_call_id": "c1"},
            transition_binding=_binding(),
        )
    storage.set(
        LedgerEntry(
            request_id=derived,
            tool="charge",
            # Match decorator call shape: charge(amount=10.0, tool_call_id="c1")
            args=[],
            kwargs={"amount": 10.0, "tool_call_id": "c1"},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
            lease_until=time.time() - 1,
            side_effect_boundary=SideEffectBoundary.MAYBE_CROSSED.value,
            external_operation_ref=pid,
        )
    )
    return derived, pid


def test_happy_path_charges_exactly_once(storage) -> None:
    provider = _FakePaymentProvider()
    charge = _make_charge(provider, storage)
    ledger_inst = get_ledger(charge)
    assert ledger_inst is not None

    with execution_scope(_scope()):
        result = charge(amount=10.0, tool_call_id="c1")
        assert result["charged"] is True
        again = charge(amount=10.0, tool_call_id="c1")
        assert again == result

    assert provider.charges == ["pi_mock_1"]
    entry = ledger_inst.get(_request_id(ledger_inst, {"amount": 10.0, "tool_call_id": "c1"}))
    assert entry is not None
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED


def test_redispatch_storm_never_double_charges(storage) -> None:
    provider = _FakePaymentProvider()
    charge = _make_charge(provider, storage)

    for _ in range(25):
        with execution_scope(_scope()):
            result = charge(amount=10.0, tool_call_id="c1")
            assert result["charged"] is True

    assert provider.charges == ["pi_mock_1"]


def test_reconcile_succeeded_marks_completed_without_recharge(storage) -> None:
    provider = _FakePaymentProvider()
    reconciler = _StripeReconciler(provider)
    charge = _make_charge(provider, storage, reconciler=reconciler)
    ledger_inst = get_ledger(charge)

    # The charge actually happened before the worker died.
    rid, pid = _seed_crashed_charge(provider, storage, status="succeeded")
    provider.charges.append(pid)

    with execution_scope(_scope()):
        result = charge(amount=10.0, tool_call_id="c1")

    assert result == {"charged": True, "payment_intent": pid}
    assert provider.charges == [pid], "reconcile COMPLETED must never re-charge"
    entry = ledger_inst.get(rid)
    assert entry is not None
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED


def test_reconcile_requires_payment_method_grants_one_recharge(storage) -> None:
    provider = _FakePaymentProvider()
    reconciler = _StripeReconciler(provider)
    charge = _make_charge(provider, storage, reconciler=reconciler)
    ledger_inst = get_ledger(charge)

    # Intent created but never charged (worker died before confirming): the
    # provider proves NOT_EXECUTED, so exactly one re-execution is granted.
    rid, _crashed_pid = _seed_crashed_charge(
        provider, storage, status="requires_payment_method"
    )

    with execution_scope(_scope()):
        result = charge(amount=10.0, tool_call_id="c1")

    assert result["charged"] is True
    assert provider.charges == ["pi_mock_2"], (
        "NOT_EXECUTED reconcile must re-run exactly once (one new charge)"
    )
    entry = ledger_inst.get(rid)
    assert entry is not None
    assert entry.resolved_terminal_outcome() == TerminalOutcome.COMPLETED


def test_reconcile_canceled_or_missing_grants_one_recharge(storage) -> None:
    for status in ("canceled", "missing"):
        provider = _FakePaymentProvider()
        reconciler = _StripeReconciler(provider)
        charge = _make_charge(provider, storage, reconciler=reconciler)

        if status == "missing":
            # The provider never saw the intent at all: seed an entry whose
            # ref points at a payment intent the store does not know.
            with execution_scope(_scope()):
                derived = ActionLedger(storage=InMemoryLedgerStorage()).derive_request_id(
                    "charge", (), {"amount": 10.0, "tool_call_id": "c1"},
                    transition_binding=_binding(),
                )
            storage.set(
                LedgerEntry(
                    request_id=derived,
                    tool="charge",
                    args=[],
                    kwargs={"amount": 10.0, "tool_call_id": "c1"},
                    status="in-flight",
                    terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
                    lease_until=time.time() - 1,
                    side_effect_boundary=SideEffectBoundary.MAYBE_CROSSED.value,
                    external_operation_ref="pi_missing_1",
                )
            )
        else:
            _seed_crashed_charge(provider, storage, status="canceled")

        with execution_scope(_scope()):
            result = charge(amount=10.0, tool_call_id="c1")

        assert result["charged"] is True
        assert len(provider.charges) == 1, (
            f"{status} reconcile must grant exactly one new charge"
        )


def test_reconcile_processing_hard_blocks_without_recharge(storage) -> None:
    provider = _FakePaymentProvider()
    reconciler = _StripeReconciler(provider)
    charge = _make_charge(provider, storage, reconciler=reconciler)

    # Charge submitted but outcome unknown: no re-execution is allowed.
    rid, pid = _seed_crashed_charge(provider, storage, status="processing")

    with execution_scope(_scope()):
        with pytest.raises(LedgerHardBlockError):
            charge(amount=10.0, tool_call_id="c1")
        with pytest.raises(LedgerHardBlockError):
            charge(amount=10.0, tool_call_id="c1")

    assert provider.charges == [], "UNKNOWN reconcile must never re-charge"
    assert provider.retrieve_calls == 2
    entry = storage.get(rid)
    assert entry is not None
    assert entry.terminal_outcome == TerminalOutcome.BLOCKED.value
    assert entry.external_operation_ref == pid


def test_operator_release_unblocks_processing_with_one_recharge(storage) -> None:
    provider = _FakePaymentProvider()
    reconciler = _StripeReconciler(provider)
    charge = _make_charge(provider, storage, reconciler=reconciler)
    ledger_inst = get_ledger(charge)

    rid, pid = _seed_crashed_charge(provider, storage, status="processing")

    with execution_scope(_scope()):
        with pytest.raises(LedgerHardBlockError):
            charge(amount=10.0, tool_call_id="c1")

    # Operator checks the provider, finds no successful charge, releases as
    # NOT_EXECUTED: exactly one re-execution.
    entry = ledger_inst.release(
        rid,
        verified="not_executed",
        by="ops@payments.example",
        reason=f"provider shows no succeeded charge for {pid}",
    )
    assert entry.operator_resolution == "not_executed"

    with execution_scope(_scope()):
        result = charge(amount=10.0, tool_call_id="c1")

    assert result["charged"] is True
    assert provider.charges == ["pi_mock_2"], (
        "operator NOT_EXECUTED release must grant exactly one new charge"
    )


def test_release_refused_on_completed_and_live_lease(storage) -> None:
    provider = _FakePaymentProvider()
    charge = _make_charge(provider, storage)
    ledger_inst = get_ledger(charge)

    with execution_scope(_scope()):
        charge(amount=10.0, tool_call_id="c1")
        rid = _request_id(ledger_inst, {"amount": 10.0, "tool_call_id": "c1"})

    with pytest.raises(LedgerReleaseRefusedError):
        ledger_inst.release(
            rid, verified="not_executed", by="ops@payments.example", reason="check"
        )


def test_reconciler_is_read_only(storage) -> None:
    provider = _FakePaymentProvider()
    reconciler = _StripeReconciler(provider)
    charge = _make_charge(provider, storage, reconciler=reconciler)

    _seed_crashed_charge(provider, storage, status="succeeded")
    before = provider.snapshot()
    charges_before = list(provider.charges)

    with execution_scope(_scope()):
        charge(amount=10.0, tool_call_id="c1")

    assert provider.snapshot() == before, "reconciler must never mutate the store"
    assert provider.charges == charges_before


def test_no_reconciler_hard_blocks_without_provider_evidence(storage) -> None:
    # No reconciler configured: an ambiguous crash must hard-block, never
    # re-charge (fail-closed default).
    provider = _FakePaymentProvider()
    charge = _make_charge(provider, storage)

    _seed_crashed_charge(provider, storage, status="processing")

    with execution_scope(_scope()):
        with pytest.raises(LedgerHardBlockError):
            charge(amount=10.0, tool_call_id="c1")

    assert provider.charges == []
