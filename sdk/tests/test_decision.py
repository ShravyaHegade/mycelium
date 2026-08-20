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
    SideEffectBoundary,
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


def test_config_policies_compose_into_the_atomic_ledger_decision() -> None:
    """Independent catalog guards cannot reject before the fenced CAS."""
    from mycelium import SecretInArgsError, get_ledger, load_config_from_string

    config = load_config_from_string(
        """
action_ledger:
  storage: memory
  tools: [send_email]
secret_args:
  enabled: true
  policy: error
entity_guard:
  enabled: true
  missing_policy: error
  tools:
    send_email:
      destinations:
        - path: recipient
          type: email
          allow:
            domains: [customer.com]
tools:
  send_email:
    side_effect_class: non_idempotent_mutate
"""
    )
    body_ran: list[bool] = []

    def send_email(recipient: str, api_key: str) -> None:
        del recipient, api_key
        body_ran.append(True)

    wrapped = config.apply_tool("send_email", send_email)
    assert getattr(wrapped, "_mycelium_atomic_decision_policy", False) is True
    assert getattr(wrapped, "_mycelium_entity_guard", False) is False
    assert getattr(wrapped, "_mycelium_secret_args", False) is False

    with pytest.raises(SecretInArgsError):
        wrapped(
            recipient="attacker@example.net",
            api_key="ghp_abcdefghijklmnopqrstuvwxyz123456",
            request_id="atomic-policy-denial",
        )

    ledger_instance = get_ledger(wrapped)
    assert ledger_instance is not None
    stored = ledger_instance.get("atomic-policy-denial")
    assert stored is not None
    # The decision CAS advanced to ATTEMPTING; compatibility error handling
    # subsequently closes the denied attempt as ABORTED.
    assert stored.effect_phase == "ABORTED"
    assert body_ran == []
    decision = Decision.from_dict(stored.decision)
    assert decision.allowed is False
    assert decision.predicate_results == {
        "destination_policy": False,
        "destructive_confirm": True,
        "secret_protection": False,
        "authority_window": True,
        "use_time_currency": True,
    }
    # The CAS record stores predicate outcomes, never the original credential.
    assert "ghp_abcdefghijklmnopqrstuvwxyz123456" not in str(stored.to_dict())


def test_rejected_secret_is_redacted_before_plugin_evaluation() -> None:
    from mycelium import SecretInArgsError, load_config_from_string

    secret = "ghp_abcdefghijklmnopqrstuvwxyz123456"
    seen: list[DecisionIntent] = []
    register_decision_predicate("observe", lambda intent, snapshot: seen.append(intent) or True)
    config = load_config_from_string(
        """
action_ledger:
  storage: memory
  tools: [send]
secret_args:
  enabled: true
  policy: error
tools:
  send:
    side_effect_class: non_idempotent_mutate
"""
    )

    def send(api_key: str) -> None:
        raise AssertionError("body must not run")

    wrapped = config.apply_tool("send", send)
    with pytest.raises(SecretInArgsError):
        wrapped(api_key=secret, request_id="plugin-secret-input")

    assert len(seen) == 1
    assert secret not in repr(seen[0])
    assert seen[0].kwargs["api_key"] == "[REDACTED]"


def test_warn_secret_is_redacted_for_plugins_but_original_reaches_body() -> None:
    from mycelium import load_config_from_string

    secret = "ghp_abcdefghijklmnopqrstuvwxyz123456"
    seen: list[DecisionIntent] = []
    body_values: list[str] = []
    register_decision_predicate("observe", lambda intent, snapshot: seen.append(intent) or True)
    config = load_config_from_string(
        """
action_ledger:
  storage: memory
  tools: [send]
secret_args:
  enabled: true
  policy: warn
  allow_fields: [api_key]
tools:
  send:
    side_effect_class: non_idempotent_mutate
"""
    )

    def send(api_key: str) -> None:
        body_values.append(api_key)

    wrapped = config.apply_tool("send", send)
    wrapped(api_key=secret, request_id="plugin-secret-warn")

    assert body_values == [secret]
    assert len(seen) == 1
    assert secret not in repr(seen[0])
    assert seen[0].kwargs["api_key"] == "[REDACTED]"


def test_allowed_reason_field_cannot_bypass_decision_sanitization() -> None:
    from mycelium import get_ledger, load_config_from_string

    secret = "ghp_abcdefghijklmnopqrstuvwxyz123456"
    register_decision_predicate(
        "unsafe_plugin",
        lambda intent, snapshot: PredicateVerdict(
            "unsafe_plugin", False, f"credential={secret}"
        ),
    )
    config = load_config_from_string(
        """
action_ledger:
  storage: memory
  tools: [charge]
secret_args:
  enabled: true
  policy: warn
  allow_fields: [reason]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    )

    def charge() -> None:
        raise AssertionError("body must not run")

    wrapped = config.apply_tool("charge", charge)
    with pytest.raises(LedgerHardBlockError):
        wrapped(request_id="allowed-reason-secret")

    ledger_instance = get_ledger(wrapped)
    assert ledger_instance is not None
    stored = ledger_instance.get("allowed-reason-secret")
    assert stored is not None
    assert secret not in str(stored.to_dict())
    assert "[REDACTED]" in str(stored.decision)


def test_atomic_policy_events_emit_only_after_successful_decision_cas(
    ledger: ActionLedger,
) -> None:
    from mycelium import DecisionPolicyBundle, apply_decision_policy, get_ledger
    from mycelium.action_ledger import ledger_sync
    from mycelium.destructive_confirm import DestructiveConfirmPolicy
    from mycelium.use_time_currency import (
        UseTimeCurrencyPolicy,
        UseTimeFactSpec,
        UseTimeToolPolicy,
        ValidatorResult,
        register_use_time_validator,
    )

    calls: list[dict[str, Any]] = []
    wrapped_ledger: ActionLedger | None = None

    class Emitter:
        def emit_event(self, **kwargs: Any) -> None:
            assert wrapped_ledger is not None
            stored = wrapped_ledger.get(kwargs["request_id"])
            assert stored is not None and stored.decision is not None
            calls.append(kwargs)

    register_use_time_validator(
        "current", lambda **kwargs: ValidatorResult(current=True, value=True)
    )
    currency = UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            "refund": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="payment.refundable",
                        subject_type="payment",
                        id_from="payment_id",
                        validator="current",
                        require={"value": True},
                    ),
                )
            )
        },
    )

    @ledger_sync(storage=ledger._storage, transition_binding=_BINDING)
    def refund(payment_id: str) -> str:
        return payment_id

    wrapped_ledger = get_ledger(refund)
    wrapped = apply_decision_policy(
        refund,
        DecisionPolicyBundle(
            destructive_policy=DestructiveConfirmPolicy(policy_version="test"),
            use_time_policy=currency,
            outcome_emitter=Emitter(),
        ),
        tool_name="refund",
    )
    with _scope():
        assert wrapped(payment_id="pay_1", request_id="policy-events") == "pay_1"

    assert [(item["event"], item["gate"]) for item in calls] == [
        ("destructive_confirm", "allowed"),
        ("use_time_currency", "allowed"),
    ]


def test_stale_decision_cas_emits_no_atomic_policy_event() -> None:
    from mycelium import DecisionPolicyBundle, apply_decision_policy
    from mycelium.action_ledger import ledger_sync
    from mycelium.destructive_confirm import DestructiveConfirmPolicy

    calls: list[dict[str, Any]] = []

    class RejectDecisionStorage(InMemoryLedgerStorage):
        def try_transition(self, entry: LedgerEntry, **kwargs: Any) -> bool:
            if kwargs.get("expected_effect_phase") == "INTENDED":
                return False
            return super().try_transition(entry, **kwargs)

    class Emitter:
        def emit_event(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    @ledger_sync(storage=RejectDecisionStorage(), transition_binding=_BINDING)
    def refund() -> None:
        raise AssertionError("body must not run")

    wrapped = apply_decision_policy(
        refund,
        DecisionPolicyBundle(
            destructive_policy=DestructiveConfirmPolicy(policy_version="test"),
            outcome_emitter=Emitter(),
        ),
        tool_name="refund",
    )
    with _scope(), pytest.raises(LedgerOutcomeAlreadySetError):
        wrapped(request_id="stale-policy-event")

    assert calls == []


def test_plugin_reason_is_sanitized_before_decision_persistence(
    ledger: ActionLedger,
) -> None:
    from mycelium.action_ledger import ledger_sync

    secret = "ghp_abcdefghijklmnopqrstuvwxyz123456"
    register_decision_predicate(
        "unsafe_plugin",
        lambda intent, snapshot: PredicateVerdict(
            "unsafe_plugin", False, f"credential={secret}"
        ),
    )

    @ledger_sync(storage=ledger._storage, transition_binding=_BINDING)
    def charge() -> None:
        raise AssertionError("body must not run")

    with _scope(), pytest.raises(LedgerHardBlockError):
        charge(request_id="plugin-secret-reason")

    stored = ledger.get("plugin-secret-reason")
    assert stored is not None
    assert secret not in str(stored.to_dict())
    assert "[REDACTED]" in str(stored.decision)


def test_destructive_grant_expiry_is_decided_at_final_boundary(
    ledger: ActionLedger,
) -> None:
    from mycelium import DecisionPolicyBundle, apply_decision_policy, get_ledger
    from mycelium.action_ledger import ledger_sync
    from mycelium.destructive_confirm import (
        DestructiveConfirmPolicy,
        DestructiveGrantError,
        DestructiveGrantSpec,
        DestructiveObjectSpec,
        DestructiveToolPolicy,
        InMemoryDestructiveGrantStore,
        issue_destructive_grant,
        reset_destructive_clock,
        set_destructive_clock,
    )
    from mycelium.transition import execution_scope

    now = {"value": 100.0}
    clock_token = set_destructive_clock(lambda: now["value"])
    store = InMemoryDestructiveGrantStore()
    policy = DestructiveConfirmPolicy(
        policy_version="test",
        tools={
            "refund": DestructiveToolPolicy(
                operation="refund",
                object=DestructiveObjectSpec(object_type="payment", id_from="payment_id"),
                grant=DestructiveGrantSpec(bind_request_id=True),
            )
        },
    )
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        request_id="expires-at-boundary",
        expires_in=5,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    body_ran: list[bool] = []

    @ledger_sync(storage=ledger._storage, transition_binding=_BINDING)
    def refund(payment_id: str) -> None:
        body_ran.append(True)

    wrapped = apply_decision_policy(
        refund,
        DecisionPolicyBundle(destructive_policy=policy, destructive_store=store),
        tool_name="refund",
    )
    wrapped_ledger = get_ledger(refund)
    assert wrapped_ledger is not None
    original_derive = wrapped_ledger.derive_request_id

    def expire_before_claim(*args: Any, **kwargs: Any) -> str:
        now["value"] = 106.0
        return original_derive(*args, **kwargs)

    wrapped_ledger.derive_request_id = expire_before_claim  # type: ignore[method-assign]
    try:
        scope = TransitionScope(
            thread_id="t", run_id="r", destructive_grants=(grant,)
        )
        with execution_scope(scope), pytest.raises(DestructiveGrantError) as caught:
            wrapped(payment_id="pay_1", request_id="expires-at-boundary")
    finally:
        wrapped_ledger.derive_request_id = original_derive  # type: ignore[method-assign]
        reset_destructive_clock(clock_token)

    assert caught.value.reason == "expired"
    assert body_ran == []
    stored = ledger.get("expires-at-boundary")
    assert stored is not None
    decision = Decision.from_dict(stored.decision)
    assert decision.predicate_results["destructive_confirm"] is False


def test_destructive_authority_adapter_runs_before_atomic_decision(
    ledger: ActionLedger,
) -> None:
    from mycelium import DecisionPolicyBundle, apply_decision_policy
    from mycelium.action_ledger import ledger_sync
    from mycelium.authority_window import (
        AuthorityValidation,
        register_authority_use_adapter,
    )
    from mycelium.destructive_confirm import (
        DestructiveConfirmPolicy,
        DestructiveGrantSpec,
        DestructiveObjectSpec,
        DestructiveToolPolicy,
        InMemoryDestructiveGrantStore,
        issue_destructive_grant,
    )
    from mycelium.transition import execution_scope

    store = InMemoryDestructiveGrantStore()
    policy = DestructiveConfirmPolicy(
        policy_version="test",
        tools={
            "refund": DestructiveToolPolicy(
                operation="refund",
                object=DestructiveObjectSpec(object_type="payment", id_from="payment_id"),
                grant=DestructiveGrantSpec(bind_request_id=True),
            )
        },
    )
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        request_id="authority-adapter-order",
        expires_in=300,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    seen: list[str] = []

    def adapter(authority: Any, **kwargs: Any) -> AuthorityValidation:
        seen.append(kwargs["phase"].value)
        return AuthorityValidation(
            decision="allowed",
            reason="valid",
            phase=kwargs["phase"].value,
            authority_kind=authority.authority_kind,
            tool=authority.tool,
            policy_version=authority.policy_version,
        )

    register_authority_use_adapter("destructive_grant", adapter)

    @ledger_sync(storage=ledger._storage, transition_binding=_BINDING)
    def refund(payment_id: str) -> str:
        return payment_id

    wrapped = apply_decision_policy(
        refund,
        DecisionPolicyBundle(destructive_policy=policy, destructive_store=store),
        tool_name="refund",
    )
    scope = TransitionScope(thread_id="t", run_id="r", destructive_grants=(grant,))
    with execution_scope(scope):
        assert wrapped(
            payment_id="pay_1", request_id="authority-adapter-order"
        ) == "pay_1"

    assert seen == ["use"]
    stored = ledger.get("authority-adapter-order")
    assert stored is not None
    assert Decision.from_dict(stored.decision).predicate_results["authority_window"]


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


def test_manual_pre_provider_mutations_require_attempting_phase(
    ledger: ActionLedger,
) -> None:
    with _scope():
        claimed = ledger.claim_side_effecting(
            "dec-manual-provider",
            "charge",
            (),
            {"request_id": "dec-manual-provider", "thread_id": "t", "run_id": "r"},
            _BINDING,
        )

    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.attach_external_operation_ref(
            claimed.request_id,
            "pi_too_early",
            expected_owner=claimed.owner,
            expected_fence=claimed.fence,
        )
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.advance_boundary(
            claimed.request_id,
            SideEffectBoundary.MAYBE_CROSSED,
            expected_owner=claimed.owner,
            expected_fence=claimed.fence,
        )

    ledger.record_decision(
        claimed.request_id,
        Decision(allowed=True).to_dict(),
        expected_owner=claimed.owner,
        expected_fence=claimed.fence,
    )
    attached = ledger.attach_external_operation_ref(
        claimed.request_id,
        "pi_after_decision",
        expected_owner=claimed.owner,
        expected_fence=claimed.fence,
    )
    advanced = ledger.advance_boundary(
        claimed.request_id,
        SideEffectBoundary.MAYBE_CROSSED,
        expected_owner=claimed.owner,
        expected_fence=claimed.fence,
    )
    assert attached.external_operation_ref == "pi_after_decision"
    assert advanced.side_effect_boundary == SideEffectBoundary.MAYBE_CROSSED.value


def test_manual_decisions_fail_closed_when_malformed_or_denied(
    ledger: ActionLedger,
) -> None:
    from mycelium import LedgerError

    with _scope():
        malformed = ledger.claim_side_effecting(
            "dec-manual-malformed",
            "charge",
            (),
            {"request_id": "dec-manual-malformed", "thread_id": "t", "run_id": "r"},
            _BINDING,
        )
    for invalid in (
        {},
        {"allowed": "true", "verdicts": [], "denied_reasons": []},
    ):
        with pytest.raises(LedgerError, match="Invalid decision"):
            ledger.record_decision(
                malformed.request_id,
                invalid,
                expected_owner=malformed.owner,
                expected_fence=malformed.fence,
            )
    assert ledger.get(malformed.request_id).effect_phase == "INTENDED"

    with _scope():
        denied = ledger.claim_side_effecting(
            "dec-manual-denied",
            "charge",
            (),
            {"request_id": "dec-manual-denied", "thread_id": "t", "run_id": "r"},
            _BINDING,
        )
    denied_decision = Decision(
        allowed=False,
        verdicts=(PredicateVerdict("policy", False, "denied"),),
        denied_reasons=("denied",),
    )
    recorded = ledger.record_decision(
        denied.request_id,
        denied_decision.to_dict(),
        expected_owner=denied.owner,
        expected_fence=denied.fence,
    )
    assert recorded.effect_phase == "ABORTED"
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.advance_boundary(
            denied.request_id,
            SideEffectBoundary.MAYBE_CROSSED,
            expected_owner=denied.owner,
            expected_fence=denied.fence,
        )
    with pytest.raises(LedgerOutcomeAlreadySetError):
        ledger.complete(
            denied.request_id,
            {"ok": True},
            expected_fence=denied.fence,
        )
    aborted = ledger.fail(
        denied.request_id,
        RuntimeError("decision denied"),
        expected_fence=denied.fence,
    )
    assert aborted.effect_phase == "ABORTED"


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
