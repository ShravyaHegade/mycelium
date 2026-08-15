"""Authority-window expiry: use-time revalidation before side effects."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from mycelium.action_ledger import InMemoryLedgerStorage, ledger_sync, side_effect
from mycelium.authority_window import (
    AuthorityExpiredError,
    AuthorityValidationPhase,
    AuthorityWindowPolicy,
    BoundAuthority,
    as_utc_datetime,
    enforce_pending_authorities_at_use,
    get_authority_decisions,
    register_authority_for_use,
    reset_authority_window_state,
    set_authority_clock,
    set_authority_window_policy,
    validate_authority,
    validate_authority_at_use,
)
from mycelium.config import ConfigError, load_config_from_string
from mycelium.destructive_confirm import (
    DestructiveConfirmPolicy,
    DestructiveGrantSpec,
    DestructiveObjectSpec,
    DestructiveToolPolicy,
    InMemoryDestructiveGrantStore,
    apply_destructive_confirm,
    issue_destructive_grant,
    reset_destructive_confirm_state,
    set_destructive_clock,
    set_destructive_grant_store,
)
from mycelium.transition import (
    SideEffectBoundary,
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
)


@pytest.fixture(autouse=True)
def store() -> Any:
    reset_destructive_confirm_state()
    reset_authority_window_state()
    grant_store = InMemoryDestructiveGrantStore()
    set_destructive_grant_store(grant_store)
    set_authority_window_policy(
        AuthorityWindowPolicy(enabled=True, use_time_check="required")
    )
    yield grant_store
    reset_destructive_confirm_state()
    reset_authority_window_state()


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="test",
        policy_version="test",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
    )


def _policy() -> DestructiveConfirmPolicy:
    return DestructiveConfirmPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="test",
        tools={
            "refund_payment": DestructiveToolPolicy(
                operation="refund",
                object=DestructiveObjectSpec(
                    object_type="payment",
                    id_from="payment_id",
                    tenant_from="tenant_id",
                ),
                grant=DestructiveGrantSpec(
                    bind_request_id=True, max_uses=1, ttl_seconds=300
                ),
            )
        },
    )


def _bound(*, expires_at: datetime, tool: str = "refund_payment") -> BoundAuthority:
    return BoundAuthority(
        authority_id="auth-1",
        authority_kind="destructive_grant",
        expires_at=expires_at,
        tool=tool,
        operation="refund",
        object_ref="payment:pay_1",
        policy_version="test",
    )


def test_valid_before_expiry_allows() -> None:
    clock = {"now": 1_000.0}
    set_authority_clock(lambda: clock["now"])
    auth = _bound(expires_at=datetime.fromtimestamp(1_100.0, tz=UTC))
    decision = validate_authority(auth, phase=AuthorityValidationPhase.AUTHORIZE)
    assert decision.decision == "allowed"
    register_authority_for_use(auth)
    use = validate_authority_at_use()
    assert use.decision == "allowed"
    assert use.phase == "use"


def test_exact_boundary_expires() -> None:
    clock = {"now": 1_000.0}
    set_authority_clock(lambda: clock["now"])
    auth = _bound(expires_at=datetime.fromtimestamp(1_000.0, tz=UTC))
    with pytest.raises(AuthorityExpiredError):
        validate_authority(auth, phase=AuthorityValidationPhase.USE)


def test_naive_timestamp_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        BoundAuthority(
            authority_id="a",
            authority_kind="test",
            expires_at=datetime(2026, 1, 1, 0, 0, 0),
            tool="t",
        )


def test_injected_clock_and_delay_between_phases() -> None:
    clock = {"now": 1_000.0}
    set_authority_clock(lambda: clock["now"])
    auth = _bound(expires_at=datetime.fromtimestamp(1_050.0, tz=UTC))
    validate_authority(auth, phase=AuthorityValidationPhase.AUTHORIZE)
    register_authority_for_use(auth)
    clock["now"] = 1_060.0
    with pytest.raises(AuthorityExpiredError) as exc:
        enforce_pending_authorities_at_use()
    assert exc.value.phase == "use"


def test_skew_narrows_never_extends() -> None:
    clock = {"now": 1_000.0}
    set_authority_clock(lambda: clock["now"])
    set_authority_window_policy(
        AuthorityWindowPolicy(
            enabled=True,
            use_time_check="required",
            clock_skew_tolerance_seconds=10,
        )
    )
    auth = _bound(expires_at=datetime.fromtimestamp(1_005.0, tz=UTC))
    with pytest.raises(AuthorityExpiredError):
        validate_authority_at_use(auth)


def test_timeless_and_omitted_unchanged() -> None:
    reset_authority_window_state()
    decision = validate_authority_at_use(None)
    assert decision.decision == "skipped"
    assert decision.reason == "timeless"


def test_model_mapping_rejected() -> None:
    with pytest.raises(AuthorityExpiredError, match="BoundAuthority"):
        validate_authority({"expires_at": 1}, phase=AuthorityValidationPhase.USE)


def test_destructive_expire_before_use_no_body(store: InMemoryDestructiveGrantStore) -> None:
    clock = {"now": 1_000.0}
    set_destructive_clock(lambda: clock["now"])
    events: list[str] = []

    def refund_payment(payment_id: str, tenant_id: str, amount: str) -> str:
        events.append("body")
        return payment_id

    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        request_id="aw-1",
        expires_in=30,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    clock["now"] = grant.expires_at - 1
    hold = threading.Event()
    release = threading.Event()

    class HoldStorage(InMemoryLedgerStorage):
        def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
            hold.set()
            release.wait(timeout=5.0)
            return super().try_claim_inflight(entry, lease_ttl=lease_ttl)

    hold_storage = HoldStorage()
    held = apply_destructive_confirm(
        ledger_sync(storage=hold_storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    err: list[BaseException] = []
    import contextvars

    ctx = contextvars.copy_context()

    def worker() -> None:
        try:
            with execution_scope(
                TransitionScope(
                    run_id="r", thread_id="t", destructive_grants=(grant,)
                )
            ):
                held(
                    payment_id="pay_1",
                    tenant_id="acme",
                    amount="SECRET",
                    request_id="aw-1",
                )
        except BaseException as exc:  # noqa: BLE001
            err.append(exc)

    thread = threading.Thread(target=lambda: ctx.run(worker))
    thread.start()
    hold.wait(timeout=5.0)
    clock["now"] = grant.expires_at
    release.set()
    thread.join(timeout=10.0)
    assert len(err) == 1
    assert isinstance(err[0], AuthorityExpiredError)
    assert events == []
    entry = hold_storage.get("aw-1")
    assert entry is None or entry.side_effect_boundary == SideEffectBoundary.NOT_CROSSED.value


def test_completed_retry_without_fresh_authority(
    store: InMemoryDestructiveGrantStore,
) -> None:
    clock = {"now": 1_000.0}
    set_destructive_clock(lambda: clock["now"])
    storage = InMemoryLedgerStorage()
    hits = {"n": 0}

    def refund_payment(payment_id: str, tenant_id: str, amount: str) -> str:
        hits["n"] += 1
        return payment_id

    wrapped = apply_destructive_confirm(
        ledger_sync(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        request_id="aw-ret",
        expires_in=60,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        assert wrapped(
            payment_id="pay_1", tenant_id="acme", amount="x", request_id="aw-ret"
        ) == "pay_1"
    clock["now"] = grant.expires_at + 100
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        assert wrapped(
            payment_id="pay_1", tenant_id="acme", amount="x", request_id="aw-ret"
        ) == "pay_1"
    assert hits["n"] == 1


def test_ambiguous_not_converted_to_permission(
    store: InMemoryDestructiveGrantStore,
) -> None:
    from mycelium.action_ledger import LedgerHardBlockError

    clock = {"now": 1_000.0}
    set_destructive_clock(lambda: clock["now"])
    storage = InMemoryLedgerStorage()

    def refund_payment(payment_id: str, tenant_id: str, amount: str) -> str:
        with side_effect():
            raise RuntimeError("boom")

    wrapped = apply_destructive_confirm(
        ledger_sync(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        request_id="aw-amb",
        expires_in=300,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        with pytest.raises(Exception):
            wrapped(
                payment_id="pay_1",
                tenant_id="acme",
                amount="x",
                request_id="aw-amb",
            )
    grant2 = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        request_id="aw-amb",
        expires_in=300,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant2,))
    ):
        with pytest.raises(LedgerHardBlockError):
            wrapped(
                payment_id="pay_1",
                tenant_id="acme",
                amount="x",
                request_id="aw-amb",
            )



def test_evidence_phases_without_payload(store: InMemoryDestructiveGrantStore) -> None:
    clock = {"now": 1_000.0}
    set_destructive_clock(lambda: clock["now"])
    storage = InMemoryLedgerStorage()
    secret = "INTERNAL_AUTHORITY_SECRET"

    def refund_payment(payment_id: str, tenant_id: str, amount: str) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        ledger_sync(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        request_id="aw-ev",
        expires_in=60,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        wrapped(
            payment_id="pay_1",
            tenant_id="acme",
            amount=secret,
            request_id="aw-ev",
        )
    decisions = get_authority_decisions()
    assert {item.phase for item in decisions} >= {"authorize", "use"}
    blob = json.dumps([item.to_dict() for item in decisions], default=str)
    assert secret not in blob
    entry = storage.get("aw-ev")
    assert entry is not None
    assert secret not in json.dumps(entry.to_dict(), default=str)


def test_sync_async_parity(store: InMemoryDestructiveGrantStore) -> None:
    clock = {"now": 1_000.0}
    set_destructive_clock(lambda: clock["now"])

    def sync_refund(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    async def async_refund(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    sync_wrapped = apply_destructive_confirm(
        sync_refund, _policy(), tool_name="refund_payment", store=store
    )
    async_wrapped = apply_destructive_confirm(
        async_refund, _policy(), tool_name="refund_payment", store=store
    )
    g1 = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        request_id="aw-s",
        expires_in=60,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    g2 = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_2",
        tenant="acme",
        request_id="aw-a",
        expires_in=60,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(g1,))
    ):
        assert sync_wrapped(payment_id="pay_1", tenant_id="acme", request_id="aw-s") == "pay_1"
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(g2,))
    ):
        assert asyncio.run(
            async_wrapped(payment_id="pay_2", tenant_id="acme", request_id="aw-a")
        ) == "pay_2"


def test_config_authority_window_and_production() -> None:
    cfg = load_config_from_string(
        """
profile: development
tools:
  refund_payment:
    side_effect_class: irreversible
destructive_confirm:
  enabled: true
  missing_policy: error
  tools:
    refund_payment:
      operation: refund
      object: {type: payment, id_from: payment_id}
      grant: {bind_request_id: true, ttl_seconds: 60, max_uses: 1}
authority_window:
  enabled: true
  use_time_check: required
  clock_skew_tolerance_seconds: 0
"""
    )
    assert cfg.authority_window is not None
    assert cfg.authority_window["use_time_check"] == "required"

    from mycelium.config import _parse_authority_window

    with pytest.raises(ConfigError, match="use-time"):
        _parse_authority_window(
            {
                "authority_window": {
                    "enabled": True,
                    "use_time_check": "optional",
                }
            },
            profile="production",
            destructive_confirm={"enabled": True},
        )

    with pytest.raises(ConfigError, match="clock_skew"):
        load_config_from_string(
            """
profile: development
authority_window:
  enabled: true
  clock_skew_tolerance_seconds: -1
"""
        )


def test_omitted_config_preserves_behavior_without_destructive() -> None:
    cfg = load_config_from_string(
        """
profile: development
tools:
  echo:
    side_effect_class: read
"""
    )
    assert cfg.authority_window is None
    assert cfg.destructive_confirm is None


def test_as_utc_datetime_epoch() -> None:
    value = as_utc_datetime(1_700_000_000.0)
    assert value.tzinfo is not None
    assert value == datetime.fromtimestamp(1_700_000_000.0, tz=UTC)


def test_mark_maybe_crossed_checks_expiry() -> None:
    from mycelium.action_ledger import mark_maybe_crossed

    clock = {"now": 1_000.0}
    set_authority_clock(lambda: clock["now"])
    auth = _bound(expires_at=datetime.fromtimestamp(1_010.0, tz=UTC))
    register_authority_for_use(auth)
    clock["now"] = 1_020.0
    with pytest.raises(AuthorityExpiredError):
        mark_maybe_crossed()


def test_negative_ttl_rejected_at_issue(store: InMemoryDestructiveGrantStore) -> None:
    with pytest.raises(ValueError, match="expires_in"):
        issue_destructive_grant(
            operation="refund",
            object_type="payment",
            object_id="pay_1",
            expires_in=-1,
            store=store,
        )


def test_policy_version_mismatch_at_use() -> None:
    clock = {"now": 1_000.0}
    set_authority_clock(lambda: clock["now"])
    auth = BoundAuthority(
        authority_id="a",
        authority_kind="destructive_grant",
        expires_at=datetime.now(UTC) + timedelta(seconds=60),
        tool="refund_payment",
        policy_version="v1",
    )
    with pytest.raises(AuthorityExpiredError, match="policy version"):
        validate_authority_at_use(auth, expected_policy_version="v2")
