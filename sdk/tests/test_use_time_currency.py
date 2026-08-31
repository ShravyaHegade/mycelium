"""Use-time currency (AF-012): decide-time facts revalidated at use."""

from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone
from typing import Any

import pytest

from mycelium.action_ledger import (
    InMemoryLedgerStorage,
    get_active_transition,
    ledger,
    ledger_sync,
    side_effect,
    side_effect_async,
)
from mycelium.authority_window import (
    AuthorityExpiredError,
    BoundAuthority,
    register_authority_for_use,
    reset_authority_window_state,
    set_authority_clock,
)
from mycelium.config import ConfigError, load_config_from_string
from mycelium.transition import (
    SideEffectBoundary,
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    dispatch_scope,
    execution_scope,
)
from mycelium.use_time_currency import (
    UseTimeCurrencyError,
    UseTimeCurrencyPolicy,
    UseTimeFact,
    UseTimeFactSpec,
    UseTimeToolPolicy,
    ValidatorResult,
    apply_use_time_currency,
    authorize_use_time_facts,
    enforce_pending_use_time_facts_at_use,
    enforce_pending_use_time_facts_at_use_async,
    enforce_use_boundary,
    get_pending_use_time_facts,
    get_use_time_decisions,
    register_fact_for_use,
    register_use_time_validator,
    reset_use_time_currency_state,
    set_use_time_clock,
    set_use_time_currency_policy,
    use_time_facts,
    use_time_fingerprint,
    value_digest,
)


@pytest.fixture(autouse=True)
def _reset() -> Any:
    reset_use_time_currency_state()
    yield
    reset_use_time_currency_state()


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="test",
        policy_version="test",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
    )


def _policy() -> UseTimeCurrencyPolicy:
    return UseTimeCurrencyPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="test",
        tools={
            "refund_payment": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="payment.refundable",
                        subject_type="payment",
                        id_from="payment_id",
                        validator="payment_state",
                        require={"value": True},
                        revision_from="payment_version",
                        max_age_seconds=30,
                    ),
                )
            )
        },
    )


def _positional_policy() -> UseTimeCurrencyPolicy:
    return UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            "apply_payment": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="payment.state",
                        subject_type="payment",
                        id_from="payment_id",
                        validator="payment_state",
                        compare_to_arg="expected_state",
                    ),
                )
            )
        },
    )


def _nested_policy(tool: str, fact_name: str, validator: str) -> UseTimeCurrencyPolicy:
    return UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            tool: UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name=fact_name,
                        subject_type="resource",
                        id_from="resource_id",
                        validator=validator,
                        require={"value": True},
                    ),
                )
            )
        },
    )


def _context_policy(tool: str) -> UseTimeCurrencyPolicy:
    return UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            tool: UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="resource.current",
                        subject_type="resource",
                        id_from="resource_id",
                        validator="resource_state",
                        bind_request_id=True,
                        bind_run_id=True,
                        bind_thread_id=True,
                    ),
                )
            )
        },
    )


def test_capture_rejects_model_mapping_subject() -> None:
    with pytest.raises(UseTimeCurrencyError, match="host-controlled"):
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id={"id": "pay_1"},  # type: ignore[arg-type]
            value=True,
        )


def test_max_age_boundary_stale() -> None:
    clock = {"now": 1_000.0}
    set_use_time_clock(lambda: clock["now"])
    set_use_time_currency_policy(_policy())

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        max_age_seconds=30,
        require_value=True,
        observed_at=datetime.fromtimestamp(1_000.0, tz=timezone.utc),
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1", "request_id": "r1"},
        policy=_policy(),
    )
    clock["now"] = 1_030.0  # age == 30 → stale (>=)
    with pytest.raises(UseTimeCurrencyError) as exc:
        enforce_pending_use_time_facts_at_use(
            kwargs={"payment_id": "pay_1", "payment_version": "1"}
        )
    assert exc.value.reason == "stale"


def test_max_age_just_under_allows() -> None:
    clock = {"now": 1_000.0}
    set_use_time_clock(lambda: clock["now"])
    set_use_time_currency_policy(_policy())

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        max_age_seconds=30,
        require_value=True,
        observed_at=datetime.fromtimestamp(1_000.0, tz=timezone.utc),
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    clock["now"] = 1_029.9
    decision = enforce_pending_use_time_facts_at_use(
        kwargs={"payment_id": "pay_1", "payment_version": "1"}
    )
    assert decision.decision == "allowed"


def test_condition_false_and_revision_mismatch() -> None:
    set_use_time_currency_policy(_policy())
    state = {"refundable": False, "version": "2"}

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(
            current=bool(state["refundable"]),
            value=state["refundable"],
            revision=str(state["version"]),
        )

    register_use_time_validator("payment_state", payment_state)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    with pytest.raises(UseTimeCurrencyError) as exc:
        enforce_pending_use_time_facts_at_use(
            kwargs={"payment_id": "pay_1", "payment_version": "1"}
        )
    assert exc.value.reason == "condition_false"

    reset_use_time_currency_state()
    set_use_time_currency_policy(_policy())
    register_use_time_validator("payment_state", payment_state)
    state["refundable"] = True
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    with pytest.raises(UseTimeCurrencyError) as exc2:
        enforce_pending_use_time_facts_at_use(
            kwargs={"payment_id": "pay_1", "payment_version": "1"}
        )
    assert exc2.value.reason == "revision_mismatch"


def test_async_validator_not_silently_run_on_sync_path() -> None:
    set_use_time_currency_policy(_policy())

    async def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    with pytest.raises(UseTimeCurrencyError) as exc:
        enforce_pending_use_time_facts_at_use(
            kwargs={"payment_id": "pay_1", "payment_version": "1"}
        )
    assert exc.value.reason == "validator_failed"


async def test_async_validator_path() -> None:
    set_use_time_currency_policy(_policy())

    async def payment_state(**_kwargs: Any) -> ValidatorResult:
        await asyncio.sleep(0)
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    decision = await enforce_pending_use_time_facts_at_use_async(
        kwargs={"payment_id": "pay_1", "payment_version": "1"}
    )
    assert decision.decision == "allowed"


def test_validator_missing_and_timeout() -> None:
    set_use_time_currency_policy(_policy())
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    with pytest.raises(UseTimeCurrencyError) as exc:
        enforce_pending_use_time_facts_at_use(
            kwargs={"payment_id": "pay_1", "payment_version": "1"}
        )
    assert exc.value.reason == "validator_missing"


async def test_async_validator_timeout() -> None:
    set_use_time_currency_policy(_policy())

    async def payment_state(**_kwargs: Any) -> ValidatorResult:
        await asyncio.sleep(1.0)
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state, timeout_seconds=0.01)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    with pytest.raises(UseTimeCurrencyError) as exc:
        await enforce_pending_use_time_facts_at_use_async(
            kwargs={"payment_id": "pay_1", "payment_version": "1"}
        )
    assert exc.value.reason == "validator_timeout"


def test_ledgered_blocks_before_body_and_maybe_crossed() -> None:
    set_use_time_currency_policy(_policy())
    state = {"refundable": True, "version": "1"}

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(
            current=bool(state["refundable"]),
            value=state["refundable"],
            revision=str(state["version"]),
        )

    register_use_time_validator("payment_state", payment_state)
    events: list[str] = []
    storage = InMemoryLedgerStorage()

    def refund_payment(payment_id: str, payment_version: str) -> str:
        events.append("body")
        with side_effect():
            return payment_id

    wrapped = apply_use_time_currency(
        ledger_sync(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
    )
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    with execution_scope(TransitionScope(run_id="r", thread_id="t")):
        assert wrapped(
            payment_id="pay_1",
            payment_version="1",
            request_id="utc-ok",
        ) == "pay_1"
        assert "body" in events

        state["refundable"] = False
        events.clear()
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_1",
            value=True,
            revision="1",
            require_value=True,
        )
        with pytest.raises(UseTimeCurrencyError):
            wrapped(
                payment_id="pay_1",
                payment_version="1",
                request_id="utc-deny",
            )
        assert "body" not in events
        entry = storage.get("utc-deny")
        assert entry is None or entry.side_effect_boundary != SideEffectBoundary.MAYBE_CROSSED


async def test_async_ledgered_parity() -> None:
    set_use_time_currency_policy(_policy())

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    storage = InMemoryLedgerStorage()

    async def refund_payment(payment_id: str, payment_version: str) -> str:
        return payment_id

    wrapped = apply_use_time_currency(
        ledger(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
    )
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    with execution_scope(TransitionScope(run_id="r", thread_id="t")):
        assert (
            await wrapped(
                payment_id="pay_1",
                payment_version="1",
                request_id="utc-async",
            )
            == "pay_1"
        )


def test_completed_return_skips_use() -> None:
    set_use_time_currency_policy(_policy())
    calls = {"n": 0}

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        calls["n"] += 1
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    storage = InMemoryLedgerStorage()

    def refund_payment(payment_id: str, payment_version: str) -> str:
        return payment_id

    wrapped = apply_use_time_currency(
        ledger_sync(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
    )
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    with execution_scope(TransitionScope(run_id="r", thread_id="t")):
        assert wrapped(
            payment_id="pay_1", payment_version="1", request_id="utc-ret"
        ) == "pay_1"
        first_calls = calls["n"]
        assert (
            wrapped(payment_id="pay_1", payment_version="1", request_id="utc-ret")
            == "pay_1"
        )
        # RETURN must not re-run use-phase validators.
        assert calls["n"] == first_calls


def test_use_boundary_revalidates_each_call() -> None:
    set_use_time_currency_policy(_policy())
    calls = {"n": 0}

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        calls["n"] += 1
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    enforce_use_boundary(kwargs={"payment_id": "pay_1", "payment_version": "1"})
    assert calls["n"] == 1
    enforce_use_boundary(kwargs={"payment_id": "pay_1", "payment_version": "1"})
    assert calls["n"] == 2


@pytest.mark.parametrize(
    ("kwargs", "tenant", "account", "reason"),
    [
        ({"account_id": "acct-1"}, "tenant-1", "acct-1", "missing"),
        ({"tenant_id": "tenant-1"}, "tenant-1", "acct-1", "missing"),
        (
            {"tenant_id": "tenant-2", "account_id": "acct-1"},
            "tenant-1",
            "acct-1",
            "tenant_mismatch",
        ),
        (
            {"tenant_id": "tenant-1", "account_id": "acct-2"},
            "tenant-1",
            "acct-1",
            "account_mismatch",
        ),
    ],
)
def test_authorize_requires_exact_configured_scope(
    kwargs: dict[str, str], tenant: str, account: str, reason: str
) -> None:
    policy = UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            "refund_payment": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="payment.refundable",
                        subject_type="payment",
                        id_from="payment_id",
                        tenant_from="tenant_id",
                        account_from="account_id",
                        validator="payment_state",
                        require={"value": True},
                    ),
                )
            )
        },
    )
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay-1",
        value=True,
        require_value=True,
        tenant=tenant,
        account=account,
    )
    with pytest.raises(UseTimeCurrencyError) as exc:
        authorize_use_time_facts(
            "refund_payment", (), {"payment_id": "pay-1", **kwargs}, policy=policy
        )
    assert exc.value.reason == reason


def test_sync_boundary_denies_fact_changed_after_body_start() -> None:
    storage = InMemoryLedgerStorage()
    state = {"current": True}
    provider_calls: list[str] = []
    request_ids: list[str] = []

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=state["current"], value=state["current"], revision="1")

    register_use_time_validator("payment_state", payment_state)

    @ledger_sync(storage=storage, transition_binding=_binding())
    def refund_payment(payment_id: str, payment_version: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        state["current"] = False
        with side_effect():
            provider_calls.append(payment_id)

    wrapped = apply_use_time_currency(refund_payment, _policy(), tool_name="refund_payment")
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    with pytest.raises(UseTimeCurrencyError):
        wrapped(payment_id="pay_1", payment_version="1", request_id="boundary-sync")
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_async_boundary_denies_fact_changed_after_body_start() -> None:
    storage = InMemoryLedgerStorage()
    state = {"current": True}
    provider_calls: list[str] = []
    request_ids: list[str] = []

    async def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=state["current"], value=state["current"], revision="1")

    register_use_time_validator("payment_state", payment_state)

    @ledger(storage=storage, transition_binding=_binding())
    async def refund_payment(payment_id: str, payment_version: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        state["current"] = False
        async with side_effect_async():
            provider_calls.append(payment_id)

    wrapped = apply_use_time_currency(refund_payment, _policy(), tool_name="refund_payment")
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )

    async def run() -> None:
        with pytest.raises(UseTimeCurrencyError):
            await wrapped(
                payment_id="pay_1", payment_version="1", request_id="boundary-async"
            )

    asyncio.run(run())
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_async_boundary_allows_current_fact_with_async_validator() -> None:
    storage = InMemoryLedgerStorage()
    provider_calls: list[str] = []
    request_ids: list[str] = []

    async def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)

    @ledger(storage=storage, transition_binding=_binding())
    async def refund_payment(payment_id: str, payment_version: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        async with side_effect_async():
            provider_calls.append(payment_id)

    wrapped = apply_use_time_currency(refund_payment, _policy(), tool_name="refund_payment")
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )
    asyncio.run(
        wrapped(payment_id="pay_1", payment_version="1", request_id="boundary-allow")
    )
    assert provider_calls == ["pay_1"]
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.CROSSED.value


def test_async_boundary_denies_expired_authority_with_async_validator() -> None:
    reset_authority_window_state()
    storage = InMemoryLedgerStorage()
    clock = {"now": 1_000.0}
    provider_calls: list[str] = []
    request_ids: list[str] = []
    set_authority_clock(lambda: clock["now"])

    async def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    register_authority_for_use(
        BoundAuthority(
            authority_id="auth-1",
            authority_kind="destructive_grant",
            expires_at=datetime.fromtimestamp(1_010.0, tz=timezone.utc),
            tool="refund_payment",
        )
    )

    @ledger(storage=storage, transition_binding=_binding())
    async def refund_payment(payment_id: str, payment_version: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        clock["now"] = 1_020.0
        async with side_effect_async():
            provider_calls.append(payment_id)

    wrapped = apply_use_time_currency(refund_payment, _policy(), tool_name="refund_payment")
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        revision="1",
        require_value=True,
    )

    async def run() -> None:
        with pytest.raises(AuthorityExpiredError):
            await wrapped(
                payment_id="pay_1", payment_version="1", request_id="boundary-expired"
            )

    asyncio.run(run())
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value
    reset_authority_window_state()


def test_sync_ledger_preserves_positional_args_at_each_use_boundary() -> None:
    storage = InMemoryLedgerStorage()
    observed: list[dict[str, Any]] = []

    def payment_state(**kwargs: Any) -> ValidatorResult:
        observed.append(kwargs)
        return ValidatorResult(current=True, value="ready")

    register_use_time_validator("payment_state", payment_state)

    @ledger_sync(storage=storage, transition_binding=_binding())
    def apply_payment(payment_id: str, expected_state: str) -> str:
        with side_effect():
            return payment_id

    wrapped = apply_use_time_currency(
        apply_payment, _positional_policy(), tool_name="apply_payment"
    )
    use_time_facts.capture(
        name="payment.state",
        subject_type="payment",
        subject_id="pay_1",
        value="ready",
    )
    assert wrapped("pay_1", "ready", request_id="positional-sync") == "pay_1"
    assert len(observed) == 2
    assert all(item["kwargs"]["payment_id"] == "pay_1" for item in observed)
    assert all(item["kwargs"]["expected_state"] == "ready" for item in observed)


def test_async_ledger_preserves_positional_args_at_each_use_boundary() -> None:
    storage = InMemoryLedgerStorage()
    observed: list[dict[str, Any]] = []

    async def payment_state(**kwargs: Any) -> ValidatorResult:
        observed.append(kwargs)
        return ValidatorResult(current=True, value="ready")

    register_use_time_validator("payment_state", payment_state)

    @ledger(storage=storage, transition_binding=_binding())
    async def apply_payment(payment_id: str, expected_state: str) -> str:
        async with side_effect_async():
            return payment_id

    wrapped = apply_use_time_currency(
        apply_payment, _positional_policy(), tool_name="apply_payment"
    )
    use_time_facts.capture(
        name="payment.state",
        subject_type="payment",
        subject_id="pay_1",
        value="ready",
    )
    assert (
        asyncio.run(wrapped("pay_1", "ready", request_id="positional-async"))
        == "pay_1"
    )
    assert len(observed) == 2
    assert all(item["kwargs"]["payment_id"] == "pay_1" for item in observed)
    assert all(item["kwargs"]["expected_state"] == "ready" for item in observed)


def test_nonledger_wrappers_preserve_positional_args_at_use() -> None:
    observed: list[dict[str, Any]] = []

    def payment_state(**kwargs: Any) -> ValidatorResult:
        observed.append(kwargs)
        return ValidatorResult(current=True, value="ready")

    register_use_time_validator("payment_state", payment_state)

    def apply_payment(payment_id: str, expected_state: str) -> str:
        return payment_id

    wrapped = apply_use_time_currency(
        apply_payment, _positional_policy(), tool_name="apply_payment"
    )
    use_time_facts.capture(
        name="payment.state",
        subject_type="payment",
        subject_id="pay_1",
        value="ready",
    )
    assert wrapped("pay_1", "ready") == "pay_1"
    assert observed[0]["kwargs"] == {
        "payment_id": "pay_1",
        "expected_state": "ready",
    }


def test_async_nonledger_wrapper_preserves_positional_args_at_use() -> None:
    observed: list[dict[str, Any]] = []

    async def payment_state(**kwargs: Any) -> ValidatorResult:
        observed.append(kwargs)
        return ValidatorResult(current=True, value="ready")

    register_use_time_validator("payment_state", payment_state)

    async def apply_payment(payment_id: str, expected_state: str) -> str:
        return payment_id

    wrapped = apply_use_time_currency(
        apply_payment, _positional_policy(), tool_name="apply_payment"
    )
    use_time_facts.capture(
        name="payment.state",
        subject_type="payment",
        subject_id="pay_1",
        value="ready",
    )
    assert asyncio.run(wrapped("pay_1", "ready")) == "pay_1"
    assert observed[0]["kwargs"] == {
        "payment_id": "pay_1",
        "expected_state": "ready",
    }


def test_nested_sync_wrapper_restores_outer_facts_for_final_boundary() -> None:
    storage = InMemoryLedgerStorage()
    outer_current = {"value": True}
    provider_calls: list[str] = []
    request_ids: list[str] = []
    outer_restored: list[bool] = []

    def outer_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=outer_current["value"], value=outer_current["value"])

    def inner_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True)

    register_use_time_validator("outer_state", outer_state)
    register_use_time_validator("inner_state", inner_state)

    def inner_tool(resource_id: str) -> str:
        return resource_id

    inner = apply_use_time_currency(
        inner_tool,
        _nested_policy("inner_tool", "inner.current", "inner_state"),
        tool_name="inner_tool",
    )

    @ledger_sync(storage=storage, transition_binding=_binding())
    def outer_tool(resource_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        assert inner("inner-1") == "inner-1"
        outer_restored.append(
            any(fact.name == "outer.current" for fact in get_pending_use_time_facts())
        )
        outer_current["value"] = False
        with side_effect():
            provider_calls.append(resource_id)

    outer = apply_use_time_currency(
        outer_tool,
        _nested_policy("outer_tool", "outer.current", "outer_state"),
        tool_name="outer_tool",
    )
    use_time_facts.capture(
        name="outer.current",
        subject_type="resource",
        subject_id="outer-1",
        value=True,
        require_value=True,
    )
    use_time_facts.capture(
        name="inner.current",
        subject_type="resource",
        subject_id="inner-1",
        value=True,
        require_value=True,
    )
    with pytest.raises(UseTimeCurrencyError):
        outer("outer-1", request_id="nested-sync")
    assert outer_restored == [True]
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_nested_async_wrapper_restores_outer_facts_for_final_boundary() -> None:
    storage = InMemoryLedgerStorage()
    outer_current = {"value": True}
    provider_calls: list[str] = []
    request_ids: list[str] = []
    outer_restored: list[bool] = []

    async def outer_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=outer_current["value"], value=outer_current["value"])

    async def inner_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True)

    register_use_time_validator("outer_state", outer_state)
    register_use_time_validator("inner_state", inner_state)

    async def inner_tool(resource_id: str) -> str:
        return resource_id

    inner = apply_use_time_currency(
        inner_tool,
        _nested_policy("inner_tool", "inner.current", "inner_state"),
        tool_name="inner_tool",
    )

    @ledger(storage=storage, transition_binding=_binding())
    async def outer_tool(resource_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        assert await inner("inner-1") == "inner-1"
        outer_restored.append(
            any(fact.name == "outer.current" for fact in get_pending_use_time_facts())
        )
        outer_current["value"] = False
        async with side_effect_async():
            provider_calls.append(resource_id)

    outer = apply_use_time_currency(
        outer_tool,
        _nested_policy("outer_tool", "outer.current", "outer_state"),
        tool_name="outer_tool",
    )
    use_time_facts.capture(
        name="outer.current",
        subject_type="resource",
        subject_id="outer-1",
        value=True,
        require_value=True,
    )
    use_time_facts.capture(
        name="inner.current",
        subject_type="resource",
        subject_id="inner-1",
        value=True,
        require_value=True,
    )

    async def run() -> None:
        with pytest.raises(UseTimeCurrencyError):
            await outer("outer-1", request_id="nested-async")

    asyncio.run(run())
    assert outer_restored == [True]
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


@pytest.mark.parametrize(
    "captured",
    [
        {"revision": "rev-1"},
        {"value": True},
    ],
)
def test_missing_validator_evidence_denies_before_provider(
    captured: dict[str, Any],
) -> None:
    storage = InMemoryLedgerStorage()
    provider_calls: list[str] = []

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True)

    register_use_time_validator("payment_state", payment_state)
    policy = UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            "apply_payment": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="payment.current",
                        subject_type="payment",
                        id_from="payment_id",
                        validator="payment_state",
                    ),
                )
            )
        },
    )

    @ledger_sync(storage=storage, transition_binding=_binding())
    def apply_payment(payment_id: str) -> None:
        with side_effect():
            provider_calls.append(payment_id)

    wrapped = apply_use_time_currency(
        apply_payment, policy, tool_name="apply_payment"
    )
    use_time_facts.capture(
        name="payment.current",
        subject_type="payment",
        subject_id="pay-1",
        **captured,
    )
    with pytest.raises(UseTimeCurrencyError) as exc:
        wrapped("pay-1", request_id="missing-evidence")
    assert exc.value.reason == "unverifiable"
    assert provider_calls == []
    entry = storage.get("missing-evidence")
    assert entry is None or entry.side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_scoped_pending_facts_are_all_revalidated() -> None:
    def scoped_state(*, fact: UseTimeFact, **_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=fact.tenant != "tenant-stale")

    register_use_time_validator("scoped_state", scoped_state)
    observed = datetime.now(timezone.utc)
    register_fact_for_use(
        UseTimeFact(
            name="resource.current",
            subject_type="resource",
            subject_id="shared-id",
            observed_at=observed,
            tenant="tenant-stale",
            account="account-1",
            validator="scoped_state",
        )
    )
    register_fact_for_use(
        UseTimeFact(
            name="resource.current",
            subject_type="resource",
            subject_id="shared-id",
            observed_at=observed,
            tenant="tenant-current",
            account="account-1",
            validator="scoped_state",
        )
    )
    assert len(get_pending_use_time_facts()) == 2
    with pytest.raises(UseTimeCurrencyError) as exc:
        enforce_pending_use_time_facts_at_use()
    assert exc.value.reason == "condition_false"


def test_captured_fact_lookup_selects_exact_scope() -> None:
    policy = UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            "read_resource": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="resource.current",
                        subject_type="resource",
                        id_from="resource_id",
                        tenant_from="tenant_id",
                        account_from="account_id",
                        validator="scoped_state",
                    ),
                )
            )
        },
    )
    for tenant in ("tenant-1", "tenant-2"):
        use_time_facts.capture(
            name="resource.current",
            subject_type="resource",
            subject_id="shared-id",
            tenant=tenant,
            account="account-1",
        )
    bound = authorize_use_time_facts(
        "read_resource",
        (),
        {
            "resource_id": "shared-id",
            "tenant_id": "tenant-1",
            "account_id": "account-1",
        },
        policy=policy,
    )
    assert len(bound) == 1
    assert bound[0].tenant == "tenant-1"


def test_recapture_replaces_older_scoped_observation_across_metadata() -> None:
    policy = UseTimeCurrencyPolicy(
        policy_version="current-policy",
        tools={
            "read_resource": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="resource.current",
                        subject_type="resource",
                        id_from="resource_id",
                        tenant_from="tenant_id",
                        account_from="account_id",
                        validator="resource_state",
                    ),
                )
            )
        },
    )

    def resource_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True)

    register_use_time_validator("resource_state", resource_state)
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="shared-id",
        tenant="tenant-1",
        account="account-1",
        value=False,
        request_id="old-request",
        run_id="old-run",
        thread_id="old-thread",
        tool="old-tool",
        policy_version="old-policy",
    )
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="shared-id",
        tenant="tenant-1",
        account="account-1",
        value=True,
        request_id="new-request",
        run_id="new-run",
        thread_id="new-thread",
        tool="new-tool",
        policy_version="current-policy",
    )
    authorize_use_time_facts(
        "read_resource",
        (),
        {
            "resource_id": "shared-id",
            "tenant_id": "tenant-1",
            "account_id": "account-1",
        },
        policy=policy,
    )
    decision = enforce_pending_use_time_facts_at_use()
    assert decision.decision == "allowed"


def test_sync_final_boundary_revalidates_distinct_same_fact_requirements() -> None:
    storage = InMemoryLedgerStorage()
    state = {"second_current": True}
    calls = {"first": 0, "second": 0}
    provider_calls: list[str] = []
    request_ids: list[str] = []

    def first_requirement(**_kwargs: Any) -> ValidatorResult:
        calls["first"] += 1
        return ValidatorResult(current=True, value=True)

    def second_requirement(**_kwargs: Any) -> ValidatorResult:
        calls["second"] += 1
        return ValidatorResult(current=state["second_current"], value=True)

    register_use_time_validator("first_requirement", first_requirement)
    register_use_time_validator("second_requirement", second_requirement)
    policy = UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            "apply_payment": UseTimeToolPolicy(
                facts=tuple(
                    UseTimeFactSpec(
                        name="payment.current",
                        subject_type="payment",
                        id_from="payment_id",
                        validator=validator,
                        require={"value": True},
                    )
                    for validator in ("first_requirement", "second_requirement")
                )
            )
        },
    )

    @ledger_sync(storage=storage, transition_binding=_binding())
    def apply_payment(payment_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        state["second_current"] = False
        with side_effect():
            provider_calls.append(payment_id)

    wrapped = apply_use_time_currency(
        apply_payment, policy, tool_name="apply_payment"
    )
    use_time_facts.capture(
        name="payment.current",
        subject_type="payment",
        subject_id="pay-1",
        value=True,
        require_value=True,
    )
    with pytest.raises(UseTimeCurrencyError):
        wrapped("pay-1", request_id="multiple-sync")
    assert calls == {"first": 2, "second": 2}
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_async_final_boundary_revalidates_distinct_same_fact_requirements() -> None:
    storage = InMemoryLedgerStorage()
    state = {"second_current": True}
    calls = {"first": 0, "second": 0}
    provider_calls: list[str] = []
    request_ids: list[str] = []

    async def first_requirement(**_kwargs: Any) -> ValidatorResult:
        calls["first"] += 1
        return ValidatorResult(current=True, value=True)

    async def second_requirement(**_kwargs: Any) -> ValidatorResult:
        calls["second"] += 1
        return ValidatorResult(current=state["second_current"], value=True)

    register_use_time_validator("first_requirement", first_requirement)
    register_use_time_validator("second_requirement", second_requirement)
    policy = UseTimeCurrencyPolicy(
        policy_version="test",
        tools={
            "apply_payment": UseTimeToolPolicy(
                facts=tuple(
                    UseTimeFactSpec(
                        name="payment.current",
                        subject_type="payment",
                        id_from="payment_id",
                        validator=validator,
                        require={"value": True},
                    )
                    for validator in ("first_requirement", "second_requirement")
                )
            )
        },
    )

    @ledger(storage=storage, transition_binding=_binding())
    async def apply_payment(payment_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        state["second_current"] = False
        async with side_effect_async():
            provider_calls.append(payment_id)

    wrapped = apply_use_time_currency(
        apply_payment, policy, tool_name="apply_payment"
    )
    use_time_facts.capture(
        name="payment.current",
        subject_type="payment",
        subject_id="pay-1",
        value=True,
        require_value=True,
    )

    async def run() -> None:
        with pytest.raises(UseTimeCurrencyError):
            await wrapped("pay-1", request_id="multiple-async")

    asyncio.run(run())
    assert calls == {"first": 2, "second": 2}
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_sync_context_binding_rejects_mismatch_before_body() -> None:
    body_calls: list[str] = []

    def resource_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True)

    register_use_time_validator("resource_state", resource_state)

    def update_resource(resource_id: str, **_kwargs: Any) -> str:
        body_calls.append(resource_id)
        return resource_id

    wrapped = apply_use_time_currency(
        update_resource, _context_policy("update_resource"), tool_name="update_resource"
    )
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="resource-1",
        request_id="request-1",
        run_id="run-1",
        thread_id="thread-1",
    )
    with execution_scope(TransitionScope(run_id="run-2", thread_id="thread-1")):
        with pytest.raises(UseTimeCurrencyError) as exc:
            wrapped("resource-1", request_id="request-1")
    assert exc.value.reason == "subject_mismatch"
    assert body_calls == []


def test_async_context_binding_rejects_missing_capture_before_body() -> None:
    body_calls: list[str] = []

    async def resource_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True)

    register_use_time_validator("resource_state", resource_state)

    async def update_resource(resource_id: str, **_kwargs: Any) -> str:
        body_calls.append(resource_id)
        return resource_id

    wrapped = apply_use_time_currency(
        update_resource, _context_policy("update_resource"), tool_name="update_resource"
    )
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="resource-1",
        request_id="request-1",
        run_id="run-1",
    )

    async def run() -> None:
        with execution_scope(TransitionScope(run_id="run-1", thread_id="thread-1")):
            with pytest.raises(UseTimeCurrencyError) as exc:
                await wrapped("resource-1", request_id="request-1")
        assert exc.value.reason == "missing"

    asyncio.run(run())
    assert body_calls == []


def test_sync_scope_switch_denies_at_final_boundary() -> None:
    storage = InMemoryLedgerStorage()
    provider_calls: list[str] = []
    request_ids: list[str] = []

    def resource_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True)

    register_use_time_validator("resource_state", resource_state)

    @ledger_sync(storage=storage, transition_binding=_binding())
    def update_resource(resource_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        with execution_scope(TransitionScope(run_id="run-2", thread_id="thread-1")):
            with side_effect():
                provider_calls.append(resource_id)

    wrapped = apply_use_time_currency(
        update_resource, _context_policy("update_resource"), tool_name="update_resource"
    )
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="resource-1",
        request_id="request-1",
        run_id="run-1",
        thread_id="thread-1",
    )
    with execution_scope(TransitionScope(run_id="run-1", thread_id="thread-1")):
        with pytest.raises(UseTimeCurrencyError) as exc:
            wrapped("resource-1", request_id="request-1")
    assert exc.value.reason == "subject_mismatch"
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_async_scope_switch_denies_at_final_boundary() -> None:
    storage = InMemoryLedgerStorage()
    provider_calls: list[str] = []
    request_ids: list[str] = []

    async def resource_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True)

    register_use_time_validator("resource_state", resource_state)

    @ledger(storage=storage, transition_binding=_binding())
    async def update_resource(resource_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        with execution_scope(TransitionScope(run_id="run-2", thread_id="thread-1")):
            async with side_effect_async():
                provider_calls.append(resource_id)

    wrapped = apply_use_time_currency(
        update_resource, _context_policy("update_resource"), tool_name="update_resource"
    )
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="resource-1",
        request_id="request-1",
        run_id="run-1",
        thread_id="thread-1",
    )

    async def run() -> None:
        with execution_scope(TransitionScope(run_id="run-1", thread_id="thread-1")):
            with pytest.raises(UseTimeCurrencyError) as exc:
                await wrapped("resource-1", request_id="request-1")
        assert exc.value.reason == "subject_mismatch"

    asyncio.run(run())
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_sync_dispatch_scope_switch_denies_at_final_boundary() -> None:
    """Captured request binding must compare against current dispatch, not authorize-time kwargs."""
    storage = InMemoryLedgerStorage()
    provider_calls: list[str] = []
    request_ids: list[str] = []

    def resource_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True)

    register_use_time_validator("resource_state", resource_state)

    @ledger_sync(storage=storage, transition_binding=_binding())
    def update_resource(resource_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        with dispatch_scope("request-B"):
            with side_effect():
                provider_calls.append(resource_id)

    wrapped = apply_use_time_currency(
        update_resource, _context_policy("update_resource"), tool_name="update_resource"
    )
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="resource-1",
        request_id="request-1",
        run_id="run-1",
        thread_id="thread-1",
    )
    with execution_scope(TransitionScope(run_id="run-1", thread_id="thread-1")):
        with dispatch_scope("request-1"):
            with pytest.raises(UseTimeCurrencyError) as exc:
                wrapped("resource-1", request_id="request-1")
    assert exc.value.reason == "subject_mismatch"
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_async_dispatch_scope_switch_denies_at_final_boundary() -> None:
    storage = InMemoryLedgerStorage()
    provider_calls: list[str] = []
    request_ids: list[str] = []

    async def resource_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True)

    register_use_time_validator("resource_state", resource_state)

    @ledger(storage=storage, transition_binding=_binding())
    async def update_resource(resource_id: str) -> None:
        active = get_active_transition()
        assert active is not None
        request_ids.append(active.request_id)
        with dispatch_scope("request-B"):
            async with side_effect_async():
                provider_calls.append(resource_id)

    wrapped = apply_use_time_currency(
        update_resource, _context_policy("update_resource"), tool_name="update_resource"
    )
    use_time_facts.capture(
        name="resource.current",
        subject_type="resource",
        subject_id="resource-1",
        request_id="request-1",
        run_id="run-1",
        thread_id="thread-1",
    )

    async def run() -> None:
        with execution_scope(TransitionScope(run_id="run-1", thread_id="thread-1")):
            with dispatch_scope("request-1"):
                with pytest.raises(UseTimeCurrencyError) as exc:
                    await wrapped("resource-1", request_id="request-1")
            assert exc.value.reason == "subject_mismatch"

    asyncio.run(run())
    assert provider_calls == []
    assert storage.get(request_ids[0]).side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_fingerprint_excludes_raw_values() -> None:
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value="SECRET_VALUE",
        revision="1",
        require_value=True,
        validator="payment_state",
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "payment_version": "1"},
        policy=_policy(),
    )
    fp = "|".join(use_time_fingerprint(get_pending_use_time_facts()))
    assert "SECRET_VALUE" not in fp
    assert value_digest("SECRET_VALUE") not in fp or "payment.refundable" in fp


def test_omitted_config_unchanged() -> None:
    cfg = load_config_from_string(
        """
profile: development
tools:
  refund_payment:
    side_effect_class: keyed_mutate
"""
    )
    assert cfg.use_time_currency is None


def test_config_rejects_unknown_keys_and_negative_max_age() -> None:
    with pytest.raises(ConfigError, match="unsupported"):
        load_config_from_string(
            """
use_time_currency:
  enabled: true
  extra: true
  tools:
    refund_payment:
      facts:
        - name: payment.refundable
          subject: {type: payment, id_from: payment_id}
          validator: payment_state
"""
        )
    with pytest.raises(ConfigError, match="max_age_seconds"):
        load_config_from_string(
            """
use_time_currency:
  enabled: true
  tools:
    refund_payment:
      facts:
        - name: payment.refundable
          subject: {type: payment, id_from: payment_id}
          validator: payment_state
          max_age_seconds: -1
"""
        )
    with pytest.raises(ConfigError, match="subject.type"):
        load_config_from_string(
            """
use_time_currency:
  enabled: true
  tools:
    refund_payment:
      facts:
        - name: payment.refundable
          subject: {id_from: payment_id}
          validator: payment_state
"""
        )


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, True, False])
def test_use_time_facts_reject_invalid_max_age(value: float) -> None:
    common = {
        "name": "record.current",
        "subject_type": "record",
        "subject_id": "record-1",
        "observed_at": datetime.now(timezone.utc),
        "max_age_seconds": value,
    }
    with pytest.raises(ValueError, match="max_age_seconds"):
        UseTimeFact(**common)
    with pytest.raises(ValueError, match="max_age_seconds"):
        UseTimeFactSpec(
            name="record.current",
            subject_type="record",
            id_from="record_id",
            validator="record_state",
            max_age_seconds=value,
        )


@pytest.mark.parametrize("value", [".nan", ".inf", "-.inf", "true", "false"])
def test_config_rejects_nonfinite_max_age(value: str) -> None:
    with pytest.raises(ConfigError, match="max_age_seconds"):
        load_config_from_string(f"""
use_time_currency:
  tools:
    lookup_record:
      facts:
        - name: record.current
          subject: {{type: record, id_from: record_id}}
          validator: record_state
          max_age_seconds: {value}
""")


def test_production_requires_missing_policy_error() -> None:
    from mycelium.config import ToolConfig, _parse_use_time_currency
    from mycelium.transition import SideEffectClass

    tools = {
        "refund_payment": ToolConfig(
            name="refund_payment",
            side_effect_class=SideEffectClass.KEYED_MUTATE,
        )
    }
    with pytest.raises(ConfigError, match="missing_policy"):
        _parse_use_time_currency(
            {
                "use_time_currency": {
                    "enabled": True,
                    "missing_policy": "warn",
                    "tools": {
                        "refund_payment": {
                            "facts": [
                                {
                                    "name": "payment.refundable",
                                    "subject": {
                                        "type": "payment",
                                        "id_from": "payment_id",
                                    },
                                    "validator": "payment_state",
                                }
                            ]
                        }
                    },
                }
            },
            profile="production",
            tools=tools,
        )


def test_provider_precondition_recorded() -> None:
    policy = UseTimeCurrencyPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="test",
        tools={
            "refund_payment": UseTimeToolPolicy(
                facts=(
                    UseTimeFactSpec(
                        name="payment.refundable",
                        subject_type="payment",
                        id_from="payment_id",
                        validator="payment_state",
                        require={"value": True},
                        provider_precondition="if_match",
                    ),
                )
            )
        },
    )
    set_use_time_currency_policy(policy)

    def payment_state(**_kwargs: Any) -> ValidatorResult:
        return ValidatorResult(current=True, value=True, revision="1")

    register_use_time_validator("payment_state", payment_state)
    use_time_facts.capture(
        name="payment.refundable",
        subject_type="payment",
        subject_id="pay_1",
        value=True,
        require_value=True,
    )
    authorize_use_time_facts(
        "refund_payment",
        (),
        {"payment_id": "pay_1", "if_match": "etag-1"},
        policy=policy,
    )
    decision = enforce_pending_use_time_facts_at_use(
        kwargs={"payment_id": "pay_1", "if_match": "etag-1"}
    )
    assert decision.provider_precondition == "if_match"
    assert decision.provider_precondition_present is True
    assert get_use_time_decisions()
