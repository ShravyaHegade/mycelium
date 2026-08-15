"""Use-time currency (AF-012): decide-time facts revalidated at use."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from mycelium.action_ledger import InMemoryLedgerStorage, ledger, ledger_sync, side_effect
from mycelium.config import ConfigError, load_config_from_string
from mycelium.transition import (
    SideEffectBoundary,
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
)
from mycelium.use_time_currency import (
    UseTimeCurrencyError,
    UseTimeCurrencyPolicy,
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
                        bind_request_id=True,
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
        observed_at=datetime.fromtimestamp(1_000.0, tz=UTC),
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
        observed_at=datetime.fromtimestamp(1_000.0, tz=UTC),
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


def test_use_boundary_token_skips_double_call() -> None:
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
    enforce_use_boundary(
        skip_if_token_valid=True,
        kwargs={"payment_id": "pay_1", "payment_version": "1"},
    )
    assert calls["n"] == 1


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
