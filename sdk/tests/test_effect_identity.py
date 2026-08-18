"""Deterministic effect identity: destination-aware transition keys."""

from __future__ import annotations

import pytest

from mycelium import (
    InMemoryLedgerStorage,
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    apply_entity_guard,
    derive_transition_key_for_call,
    enforce_entity_guard,
    execution_scope,
    ledger_sync,
)
from mycelium.entity_guard import (
    DEST_EMAIL,
    ApprovedDestination,
    DestinationAllow,
    DestinationSpec,
    EntityDecision,
    EntityGuardPolicy,
    ToolDestinationPolicy,
    reset_entity_guard_state,
)
from mycelium.transition import (
    build_transition_preimage,
    derive_effect_id,
    derive_effect_id_for_call,
)


@pytest.fixture(autouse=True)
def _reset_entity_guard_state() -> None:
    reset_entity_guard_state()
    yield
    reset_entity_guard_state()


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="payment-agent",
        policy_version="2026.07.1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _email_policy() -> EntityGuardPolicy:
    return EntityGuardPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="test",
        tools={
            "send_email": ToolDestinationPolicy(
                destinations=(
                    DestinationSpec(
                        path="recipient",
                        dest_type=DEST_EMAIL,
                        allow=DestinationAllow(
                            addresses=frozenset(
                                {
                                    "billing@customer.com",
                                    "ops@customer.com",
                                }
                            ),
                            domains=frozenset({"customer.com"}),
                        ),
                    ),
                )
            ),
        },
    )


def test_effect_id_aliases_exported_from_package_root() -> None:
    from mycelium import derive_effect_id as root_effect_id
    from mycelium import derive_effect_id_for_call as root_effect_id_for_call

    assert root_effect_id is derive_effect_id
    assert root_effect_id_for_call is derive_effect_id_for_call


def test_v2_preimage_includes_empty_destination_by_default() -> None:
    preimage = build_transition_preimage(
        scope=TransitionScope(thread_id="t1", run_id="r1", node="pay"),
        dispatch_id="call_abc",
        tool="send_payment",
        args=(100.0,),
        kwargs={"recipient": "acct_1"},
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        agent_id="payment-agent",
        policy_version="2026.07.1",
    )
    assert preimage["destination"] == []


def test_effect_id_aliases_transition_key() -> None:
    preimage = build_transition_preimage(
        scope=TransitionScope(thread_id="t1", run_id="r1"),
        dispatch_id="call_1",
        tool="send_payment",
        args=(),
        kwargs={"amount": 10},
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        agent_id="agent",
        policy_version="1",
        destination=("email:billing@customer.com",),
    )
    assert derive_effect_id(preimage) == derive_effect_id(preimage)
    assert len(derive_effect_id(preimage)) == 64


def test_different_destination_produces_different_effect_id() -> None:
    binding = _binding()
    base = {"amount": 10.0, "tool_call_id": "call_1"}
    _, kwargs_a, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "billing@customer.com", **base},
        policy=_email_policy(),
    )
    _, kwargs_b, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "ops@customer.com", **base},
        policy=_email_policy(),
    )
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        key_a = derive_effect_id_for_call("send_email", (), kwargs_a, binding)
        key_b = derive_effect_id_for_call("send_email", (), kwargs_b, binding)
    assert key_a != key_b


def test_entity_guard_email_canonicalization_stabilizes_effect_id() -> None:
    binding = _binding()
    base = {"amount": 10.0, "tool_call_id": "call_1"}
    _, kwargs_lower, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "billing@customer.com", **base},
        policy=_email_policy(),
    )
    _, kwargs_upper, _ = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "Billing@Customer.COM", **base},
        policy=_email_policy(),
    )
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        key_lower = derive_effect_id_for_call(
            "send_email", (), kwargs_lower, binding
        )
        key_upper = derive_effect_id_for_call(
            "send_email", (), kwargs_upper, binding
        )
    assert key_lower == key_upper


def test_same_destination_redispatch_reuses_effect_id() -> None:
    binding = _binding()
    executions: list[str] = []

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def send_email(recipient: str, amount: float) -> dict[str, str]:
        executions.append(recipient)
        return {"recipient": recipient, "status": "sent"}

    wrapped = apply_entity_guard(send_email, _email_policy(), tool_name="send_email")
    kwargs = {
        "recipient": "billing@customer.com",
        "amount": 10.0,
        "tool_call_id": "call_email_1",
    }

    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        r1 = wrapped(**kwargs)
        r2 = wrapped(**kwargs)

    assert len(executions) == 1
    assert r1 == r2


def test_effect_id_for_call_matches_transition_key_for_call() -> None:
    binding = _binding()
    kwargs = {"amount": 10.0, "tool_call_id": "call_1"}
    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-1")):
        assert derive_effect_id_for_call("send_payment", (), kwargs, binding) == (
            derive_transition_key_for_call("send_payment", (), kwargs, binding)
        )


def test_explicit_destination_in_preimage() -> None:
    decision = EntityDecision(
        tool="send_email",
        destinations=(
            ApprovedDestination(
                path="recipient",
                dest_class=DEST_EMAIL,
                entity="billing@customer.com",
            ),
        ),
        policy_version="test",
        decision="allow",
    )
    preimage = build_transition_preimage(
        scope=TransitionScope(thread_id="t1", run_id="r1"),
        dispatch_id="call_1",
        tool="send_email",
        args=(),
        kwargs={"recipient": "billing@customer.com"},
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        agent_id="agent",
        policy_version="1",
        destination=("email:billing@customer.com",),
    )
    assert preimage["destination"] == ["email:billing@customer.com"]
    del decision
