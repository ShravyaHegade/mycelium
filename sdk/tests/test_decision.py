"""Single atomic decision point (Change 2 of the effect-commit protocol).

The decision is evaluated over pure ``(intent, snapshot)`` predicates and
recorded atomically with the ``INTENDED -> ATTEMPTING`` transition, under the
same fenced compare-and-swap that guards every in-flight mutation. These tests
verify:

  1. The Decision value object and its verdicts survive serde.
  2. The DecisionEngine evaluates predicates in registration order and is
     immutable to concurrent registration at eval time.
  3. A decision is recorded on a successful wrapper-path boundary advance.
  4. A host-registered predicate is evaluated and recorded end-to-end, and a
     denied predicate hard-blocks with the decision still recorded.
  5. A stale-fence worker cannot record a decision (cannot smuggle in an effect
     the current-fence decision would have denied).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

import pytest

from mycelium import (
    ActionLedger,
    Decision,
    DecisionEngine,
    DecisionIntent,
    DecisionSnapshot,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    LedgerOutcomeAlreadySetError,
    PredicateVerdict,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    build_snapshot,
    get_decision_engine,
    register_decision_predicate,
    reset_decision_engine,
)

_BINDING = ToolTransitionBinding.for_tool(
    agent_id="test",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)

_SCOPE = TransitionScope(thread_id="t", run_id="r")


def _scope():
    from mycelium.transition import execution_scope

    return execution_scope(_SCOPE)


@pytest.fixture(autouse=True)
def _clean_engine():
    from mycelium.authority_window import reset_authority_window_state
    from mycelium.use_time_currency import reset_use_time_currency_state

    reset_decision_engine()
    reset_authority_window_state()
    reset_use_time_currency_state()
    yield
    reset_decision_engine()
    reset_authority_window_state()
    reset_use_time_currency_state()


@pytest.fixture
def ledger() -> ActionLedger:
    return ActionLedger(storage=InMemoryLedgerStorage())


def _intent(tool: str = "charge") -> DecisionIntent:
    return DecisionIntent(tool=tool, request_id="r1")


# ---------------------------------------------------------------------------
# Value object serde
# ---------------------------------------------------------------------------


def test_decision_round_trips_through_dict() -> None:
    decision = Decision(
        allowed=False,
        verdicts=(
            PredicateVerdict(name="a", allowed=True),
            PredicateVerdict(name="b", allowed=False, reason="nope"),
        ),
        denied_reasons=("nope",),
    )
    restored = Decision.from_dict(decision.to_dict())
    assert restored == decision
    assert restored.predicate_results == {"a": True, "b": False}
    assert restored.denied_reasons == ("nope",)


def test_ledger_entry_decision_round_trips_and_defaults_to_none() -> None:
    entry = LedgerEntry(
        request_id="req",
        tool="charge",
        args=[],
        kwargs={},
        status="in-flight",
    )
    assert entry.decision is None
    assert entry.effect_phase == "INTENDED"
    decision = Decision(allowed=True, verdicts=(PredicateVerdict("x", True),))
    stamped = replace(entry, decision=decision.to_dict())
    round_tripped = LedgerEntry.from_dict(stamped.to_dict())
    assert round_tripped.decision == decision.to_dict()
    assert round_tripped.effect_phase == "INTENDED"

    legacy = entry.to_dict()
    del legacy["decision"]
    assert LedgerEntry.from_dict(legacy).decision is None


# ---------------------------------------------------------------------------
# DecisionEngine
# ---------------------------------------------------------------------------


def test_engine_evaluates_in_registration_order_and_ands_results() -> None:
    engine = DecisionEngine()
    seen: list[str] = []

    def allow(name: str):
        def _p(intent: DecisionIntent, snapshot: DecisionSnapshot) -> PredicateVerdict:
            seen.append(name)
            return PredicateVerdict(name=name, allowed=True)

        return _p

    engine.register("first", allow("first"))
    engine.register("second", allow("second"))
    decision = engine.evaluate(_intent(), DecisionSnapshot())
    assert seen == ["first", "second"]
    assert decision.allowed is True
    assert [v.name for v in decision.verdicts] == ["first", "second"]


def test_engine_collects_denied_reasons_and_bool_shorthand() -> None:
    engine = DecisionEngine()
    engine.register("ok", lambda intent, snapshot: True)
    engine.register(
        "deny",
        lambda intent, snapshot: PredicateVerdict("deny", False, "too much"),
    )
    decision = engine.evaluate(_intent(), DecisionSnapshot())
    assert decision.allowed is False
    assert decision.denied_reasons == ("too much",)
    assert decision.predicate_results == {"ok": True, "deny": False}


def test_engine_snapshot_is_immutable_during_evaluate() -> None:
    engine = DecisionEngine()

    def register_more(intent: DecisionIntent, snapshot: DecisionSnapshot) -> bool:
        engine.register("late", lambda i, s: True)
        return True

    engine.register("first", register_more)
    decision = engine.evaluate(_intent(), DecisionSnapshot())
    assert [v.name for v in decision.verdicts] == ["first"]
    assert "late" in engine.registered_names()


def test_builtin_predicates_registered_by_default() -> None:
    names = get_decision_engine().registered_names()
    assert "authority_window" in names
    assert "use_time_currency" in names


# ---------------------------------------------------------------------------
# Recorded atomically with the boundary transition (wrapper path)
# ---------------------------------------------------------------------------


def test_decision_recorded_on_successful_boundary_advance(ledger: ActionLedger) -> None:
    from mycelium.action_ledger import ledger_sync

    storage = ledger._storage

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, Any]:
        return {"charged": amount}

    with _scope():
        charge(5, request_id="dec-ok")

    stored = ledger.get("dec-ok")
    assert stored is not None
    assert stored.terminal_outcome == TerminalOutcome.COMPLETED.value
    assert stored.effect_phase == "COMMITTED"
    assert stored.decision is not None
    decision = Decision.from_dict(stored.decision)
    assert decision.allowed is True
    # Built-in authority + currency predicates are always evaluated.
    assert set(decision.predicate_results) >= {"authority_window", "use_time_currency"}


def test_plugin_predicate_evaluated_and_recorded_end_to_end(
    ledger: ActionLedger,
) -> None:
    from mycelium.action_ledger import ledger_sync

    storage = ledger._storage
    seen: list[DecisionIntent] = []

    def amount_policy(intent: DecisionIntent, snapshot: DecisionSnapshot) -> PredicateVerdict:
        seen.append(intent)
        amount = intent.kwargs.get("amount", 0)
        return PredicateVerdict(
            name="amount_policy",
            allowed=amount <= 100,
            reason=None if amount <= 100 else "amount too large",
        )

    register_decision_predicate("amount_policy", amount_policy)

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, Any]:
        return {"charged": amount}

    with _scope():
        charge(amount=50, request_id="dec-plugin")

    assert len(seen) == 1
    assert seen[0].tool == "charge"
    assert seen[0].kwargs.get("amount") == 50

    stored = ledger.get("dec-plugin")
    assert stored is not None
    decision = Decision.from_dict(stored.decision)
    assert decision.allowed is True
    assert decision.predicate_results["amount_policy"] is True


def test_plugin_denial_hard_blocks_with_decision_recorded(
    ledger: ActionLedger,
) -> None:
    from mycelium.action_ledger import ledger_sync

    storage = ledger._storage
    body_ran: list[int] = []

    def deny_over_100(intent: DecisionIntent, snapshot: DecisionSnapshot) -> PredicateVerdict:
        amount = intent.kwargs.get("amount", 0)
        return PredicateVerdict(
            name="amount_policy",
            allowed=amount <= 100,
            reason=None if amount <= 100 else "amount too large",
        )

    register_decision_predicate("amount_policy", deny_over_100)

    @ledger_sync(storage=storage, transition_binding=_BINDING)
    def charge(amount: int) -> dict[str, Any]:
        body_ran.append(amount)
        return {"charged": amount}

    with _scope():
        with pytest.raises(LedgerHardBlockError):
            charge(amount=250, request_id="dec-denied")

    # Body never ran: the decision denied before the effect fired.
    assert body_ran == []
    stored = ledger.get("dec-denied")
    assert stored is not None
    decision = Decision.from_dict(stored.decision)
    assert decision.allowed is False
    assert decision.predicate_results["amount_policy"] is False
    assert "amount too large" in decision.denied_reasons


def test_authority_denial_is_recorded_before_sync_abort(
    ledger: ActionLedger,
) -> None:
    from mycelium.action_ledger import ledger_sync
    from mycelium.authority_window import (
        AuthorityExpiredError,
        BoundAuthority,
        register_authority_for_use,
        set_authority_clock,
    )

    body_ran: list[bool] = []
    set_authority_clock(lambda: 2_000.0)
    register_authority_for_use(
        BoundAuthority(
            authority_id="expired-auth",
            authority_kind="test",
            expires_at=datetime.fromtimestamp(1_000.0, tz=timezone.utc),
            tool="charge",
        )
    )

    @ledger_sync(storage=ledger._storage, transition_binding=_BINDING)
    def charge(amount: int) -> None:
        body_ran.append(True)

    with _scope(), pytest.raises(AuthorityExpiredError):
        charge(amount=5, request_id="dec-authority-denied")

    stored = ledger.get("dec-authority-denied")
    assert stored is not None
    assert body_ran == []
    decision = Decision.from_dict(stored.decision)
    assert decision.allowed is False
    assert decision.predicate_results["authority_window"] is False
    assert "expired" in decision.denied_reasons


async def test_currency_denial_is_recorded_before_async_abort(
    ledger: ActionLedger,
) -> None:
    from mycelium.action_ledger import ledger as ledger_async
    from mycelium.use_time_currency import (
        UseTimeCurrencyError,
        UseTimeFact,
        ValidatorResult,
        register_fact_for_use,
        register_use_time_validator,
    )

    body_ran: list[bool] = []
    register_use_time_validator(
        "stale_state",
        lambda **kwargs: ValidatorResult(current=False, reason="changed"),
    )
    register_fact_for_use(
        UseTimeFact(
            name="charge.current",
            subject_type="charge",
            subject_id="charge-1",
            observed_at=datetime.now(timezone.utc),
            tool="charge",
            validator="stale_state",
        )
    )

    @ledger_async(storage=ledger._storage, transition_binding=_BINDING)
    async def charge(amount: int) -> None:
        body_ran.append(True)

    with _scope(), pytest.raises(UseTimeCurrencyError):
        await charge(amount=5, request_id="dec-currency-denied")

    stored = ledger.get("dec-currency-denied")
    assert stored is not None
    assert body_ran == []
    decision = Decision.from_dict(stored.decision)
    assert decision.allowed is False
    assert decision.predicate_results["use_time_currency"] is False
    assert "changed" in decision.denied_reasons


# ---------------------------------------------------------------------------
# Stale-fence rejection
# ---------------------------------------------------------------------------


def test_stale_fence_worker_cannot_record_decision(ledger: ActionLedger) -> None:
    """A superseded worker (stale fence) cannot stamp a decision — so it cannot
    smuggle in an effect the current-fence decision would have denied."""
    claimed = ledger.claim("dec-fence", "charge", (), {})
    assert claimed.fence == 1

    # Simulate a takeover: the stored fence moves on to 2.
    storage = ledger._storage
    current = storage.get("dec-fence")
    storage.set(replace(current, fence=2))

    decision = Decision(
        allowed=False,
        verdicts=(PredicateVerdict("authority_window", False, "expired"),),
        denied_reasons=("expired",),
    )
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.record_decision(
            "dec-fence",
            decision.to_dict(),
            expected_owner=claimed.owner,
            expected_fence=1,
        )
    # No decision was recorded under the stale fence.
    assert storage.get("dec-fence").decision is None

    # The current holder (fence 2) can record.
    ledger.record_decision(
        "dec-fence",
        decision.to_dict(),
        expected_owner=claimed.owner,
        expected_fence=2,
    )
    assert storage.get("dec-fence").decision == decision.to_dict()


def test_decision_atomically_advances_phase_once(ledger: ActionLedger) -> None:
    claimed = ledger.claim("dec-phase", "charge", (), {})
    decision = Decision(allowed=True).to_dict()
    attempting = ledger.record_decision(
        "dec-phase",
        decision,
        expected_owner=claimed.owner,
        expected_fence=claimed.fence,
    )
    assert attempting.effect_phase == "ATTEMPTING"
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.record_decision(
            "dec-phase",
            decision,
            expected_owner=claimed.owner,
            expected_fence=claimed.fence,
        )


def test_manual_completion_requires_fenced_attempting_phase(
    ledger: ActionLedger,
) -> None:
    from mycelium import LedgerError

    with _scope():
        claimed = ledger.claim_side_effecting(
            "dec-manual",
            "charge",
            (),
            {"request_id": "dec-manual", "thread_id": "t", "run_id": "r"},
            _BINDING,
        )
    with pytest.raises(LedgerError, match="requires the claim fence"):
        ledger.complete("dec-manual", {"ok": True})
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(
            "dec-manual",
            {"ok": True},
            expected_fence=claimed.fence,
        )

    decision = Decision(allowed=True).to_dict()
    ledger.record_decision(
        "dec-manual",
        decision,
        expected_owner=claimed.owner,
        expected_fence=claimed.fence,
    )
    completed = ledger.complete(
        "dec-manual",
        {"ok": True},
        expected_fence=claimed.fence,
    )
    assert completed.effect_phase == "COMMITTED"


async def test_decision_recorded_on_async_boundary_advance(
    ledger: ActionLedger,
) -> None:
    from mycelium.action_ledger import ledger as ledger_async

    storage = ledger._storage
    seen: list[str] = []

    def tag(intent: DecisionIntent, snapshot: DecisionSnapshot) -> PredicateVerdict:
        seen.append(intent.tool)
        return PredicateVerdict("tag", True)

    register_decision_predicate("tag", tag)

    @ledger_async(storage=storage, transition_binding=_BINDING)
    async def charge(amount: int) -> dict[str, Any]:
        return {"charged": amount}

    with _scope():
        await charge(amount=7, request_id="dec-async")

    assert seen == ["charge"]
    stored = ledger.get("dec-async")
    assert stored is not None
    decision = Decision.from_dict(stored.decision)
    assert decision.allowed is True
    assert decision.predicate_results["tag"] is True


def test_build_snapshot_reads_pending_facts_purely() -> None:
    intent = _intent()
    snapshot = build_snapshot(intent)
    assert isinstance(snapshot, DecisionSnapshot)
    assert snapshot.authority_facts == ()
    assert snapshot.use_time_facts == ()
