"""Destructive-confirm: host grants, not tool permission, authorize the object."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

import pytest

from mycelium import (
    PAYLOAD_OMITTED,
    ConfigError,
    DestructiveGrant,
    DestructiveGrantError,
    InMemoryLedgerStorage,
    apply_destructive_confirm,
    enforce_destructive_confirm,
    issue_destructive_grant,
    ledger_sync,
    load_config_from_string,
    register_destructive_object_canonicalizer,
    side_effect,
)
from mycelium.action_ledger import ARGS_DRIFT_HARD
from mycelium.destructive_confirm import (
    DestructiveConfirmPolicy,
    DestructiveGrantSpec,
    DestructiveObjectSpec,
    DestructiveToolPolicy,
    FileDestructiveGrantStore,
    InMemoryDestructiveGrantStore,
    canonicalize_object_id,
    reset_destructive_confirm_state,
    sanitize_destructive_evidence,
    set_destructive_clock,
    set_destructive_grant_store,
)
from mycelium.doctor.types import DoctorStatus
from mycelium.transition import (
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
)


@pytest.fixture(autouse=True)
def _reset() -> None:
    reset_destructive_confirm_state()
    yield
    reset_destructive_confirm_state()


def _policy(**overrides: Any) -> DestructiveConfirmPolicy:
    spec = DestructiveObjectSpec(
        object_type="payment",
        id_from="payment_id",
        tenant_from="tenant_id",
        case_sensitive=True,
    )
    grant = DestructiveGrantSpec(bind_request_id=True, max_uses=1, ttl_seconds=300)
    tools = {
        "refund_payment": DestructiveToolPolicy(
            operation="refund", object=spec, grant=grant
        )
    }
    tools.update(overrides)
    return DestructiveConfirmPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="test",
        tools=tools,
    )


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="test",
        policy_version="test",
        side_effect_class=SideEffectClass.IRREVERSIBLE,
    )


def _issue(**kwargs: Any) -> DestructiveGrant:
    store = get_or_store()
    defaults = dict(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        expires_in=300,
        max_uses=1,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )
    defaults.update(kwargs)
    return issue_destructive_grant(**defaults)


def get_or_store() -> InMemoryDestructiveGrantStore:
    from mycelium.destructive_confirm import get_destructive_grant_store

    store = get_destructive_grant_store()
    if store is None:
        store = InMemoryDestructiveGrantStore()
        set_destructive_grant_store(store)
    return store  # type: ignore[return-value]


def test_missing_grant_blocks_before_claim() -> None:
    events: list[str] = []
    storage = InMemoryLedgerStorage()

    def refund_payment(payment_id: str, tenant_id: str, amount: str) -> str:
        events.append("body")
        return payment_id

    class Rec(InMemoryLedgerStorage):
        def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
            events.append("claim")
            return super().try_claim_inflight(entry, lease_ttl=lease_ttl)

    storage = Rec()
    wrapped = apply_destructive_confirm(
        ledger_sync(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
        store=get_or_store(),
    )
    with execution_scope(TransitionScope(run_id="r", thread_id="t")):
        with pytest.raises(DestructiveGrantError, match="missing"):
            wrapped(payment_id="pay_1", tenant_id="acme", amount="10", request_id="dc-miss")
    assert events == []


def test_exact_grant_permits_one_execution_and_retry_reuses() -> None:
    events: list[str] = []
    store = get_or_store()
    grant = _issue(request_id="dc-ok", object_id="pay_1")

    def refund_payment(payment_id: str, tenant_id: str, amount: str) -> str:
        events.append("body")
        return f"ok:{payment_id}"

    wrapped = apply_destructive_confirm(
        ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_binding())(
            refund_payment
        ),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        first = wrapped(
            payment_id="pay_1", tenant_id="acme", amount="10", request_id="dc-ok"
        )
        second = wrapped(
            payment_id="pay_1", tenant_id="acme", amount="10", request_id="dc-ok"
        )
    assert first == second == "ok:pay_1"
    assert events == ["body"]
    rec = store.get(grant.grant_id)
    assert rec is not None
    assert rec["uses_remaining"] == 0


def test_wrong_operation_and_object_and_tenant_block() -> None:
    store = get_or_store()
    grant = _issue(request_id="dc-mismatch", object_id="pay_1", tenant="acme")
    policy = _policy(
        cancel_payment=DestructiveToolPolicy(
            operation="cancel",
            object=DestructiveObjectSpec(
                object_type="payment", id_from="payment_id", tenant_from="tenant_id"
            ),
            grant=DestructiveGrantSpec(bind_request_id=True, max_uses=1, ttl_seconds=300),
        )
    )

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    def cancel_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    refund = apply_destructive_confirm(
        refund_payment, policy, tool_name="refund_payment", store=store
    )
    cancel = apply_destructive_confirm(
        cancel_payment, policy, tool_name="cancel_payment", store=store
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        with pytest.raises(DestructiveGrantError) as wrong_op:
            cancel(payment_id="pay_1", tenant_id="acme", request_id="dc-mismatch")
        assert wrong_op.value.reason in {"mismatched", "identity_drift"}
        with pytest.raises(DestructiveGrantError) as wrong_obj:
            refund(payment_id="pay_2", tenant_id="acme", request_id="dc-mismatch")
        assert wrong_obj.value.reason in {"mismatched", "identity_drift"}
        with pytest.raises(DestructiveGrantError) as wrong_tenant:
            refund(payment_id="pay_1", tenant_id="other", request_id="dc-mismatch")
        assert wrong_tenant.value.reason == "mismatched"


def test_nested_argument_path() -> None:
    store = get_or_store()
    policy = DestructiveConfirmPolicy(
        enabled=True,
        policy_version="test",
        tools={
            "refund_payment": DestructiveToolPolicy(
                operation="refund",
                object=DestructiveObjectSpec(
                    object_type="payment",
                    id_from="payment.id",
                    tenant_from="payment.tenant",
                ),
                grant=DestructiveGrantSpec(bind_request_id=True, max_uses=1, ttl_seconds=60),
            )
        },
    )
    grant = _issue(request_id="dc-nested", object_id="pay_n", tenant="acme")

    def refund_payment(payment: dict[str, str], **_: Any) -> str:
        return payment["id"]

    wrapped = apply_destructive_confirm(
        refund_payment, policy, tool_name="refund_payment", store=store
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        assert (
            wrapped(payment={"id": "pay_n", "tenant": "acme"}, request_id="dc-nested")
            == "pay_n"
        )


def test_canonicalization_rejects_bypass_forms() -> None:
    with pytest.raises(Exception):
        canonicalize_object_id("pay_1/../pay_2", object_type="payment")
    with pytest.raises(Exception):
        canonicalize_object_id("pay%31", object_type="payment")
    with pytest.raises(Exception):
        canonicalize_object_id("http://evil/pay_1", object_type="payment")
    with pytest.raises(Exception):
        canonicalize_object_id(" pay_1", object_type="payment")
    with pytest.raises(Exception):
        canonicalize_object_id("", object_type="payment")
    with pytest.raises(Exception):
        canonicalize_object_id(123, object_type="payment")  # type: ignore[arg-type]
    register_destructive_object_canonicalizer("payment", lambda value: str(value).strip())
    assert canonicalize_object_id("pay_1", object_type="payment") == "pay_1"


def test_custom_canonicalizer_runs_before_claim() -> None:
    store = get_or_store()
    seen: list[str] = []

    def canon(value: Any) -> str:
        seen.append(str(value))
        return str(value)

    register_destructive_object_canonicalizer("payment", canon)
    grant = _issue(request_id="dc-canon", object_id="pay_1")

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        refund_payment, _policy(), tool_name="refund_payment", store=store
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-canon")
    assert "pay_1" in seen


def test_expiry_uses_injectable_clock() -> None:
    store = get_or_store()
    clock = {"now": 100.0}
    set_destructive_clock(lambda: clock["now"])
    grant = _issue(request_id="dc-exp", expires_in=10)

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        refund_payment, _policy(), tool_name="refund_payment", store=store
    )
    clock["now"] = grant.expires_at
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        with pytest.raises(DestructiveGrantError) as exc:
            wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-exp")
    assert exc.value.reason == "expired"


def test_exhausted_and_multi_use() -> None:
    store = get_or_store()
    policy = _policy()
    policy = DestructiveConfirmPolicy(
        enabled=True,
        policy_version="test",
        tools={
            "refund_payment": DestructiveToolPolicy(
                operation="refund",
                object=DestructiveObjectSpec(
                    object_type="payment", id_from="payment_id", tenant_from="tenant_id"
                ),
                grant=DestructiveGrantSpec(bind_request_id=False, max_uses=2, ttl_seconds=300),
            )
        },
    )
    grant = _issue(max_uses=2, bind_request_id=False)

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        refund_payment, policy, tool_name="refund_payment", store=store
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        wrapped(payment_id="pay_1", tenant_id="acme", request_id="u1")
        wrapped(payment_id="pay_1", tenant_id="acme", request_id="u2")
        with pytest.raises(DestructiveGrantError) as exc:
            wrapped(payment_id="pay_1", tenant_id="acme", request_id="u3")
    assert exc.value.reason == "exhausted"


def test_concurrent_one_use() -> None:
    store = get_or_store()
    policy = DestructiveConfirmPolicy(
        enabled=True,
        policy_version="test",
        tools={
            "refund_payment": DestructiveToolPolicy(
                operation="refund",
                object=DestructiveObjectSpec(
                    object_type="payment", id_from="payment_id", tenant_from="tenant_id"
                ),
                grant=DestructiveGrantSpec(max_uses=1, ttl_seconds=300),
            )
        },
    )
    grant = _issue(bind_request_id=False)
    hits = {"n": 0}
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        with lock:
            hits["n"] += 1
        return payment_id

    wrapped = apply_destructive_confirm(
        refund_payment, policy, tool_name="refund_payment", store=store
    )

    def worker(rid: str) -> None:
        barrier.wait()
        try:
            with execution_scope(
                TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
            ):
                wrapped(payment_id="pay_1", tenant_id="acme", request_id=rid)
        except DestructiveGrantError:
            return

    threads = [
        threading.Thread(target=worker, args=("c1",)),
        threading.Thread(target=worker, args=("c2",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert hits["n"] == 1


def test_identity_drift_same_request_id() -> None:
    store = get_or_store()
    grant = _issue(request_id="dc-drift", object_id="pay_a")
    storage = InMemoryLedgerStorage()

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        ledger_sync(
            storage=storage,
            transition_binding=_binding(),
            on_args_drift=ARGS_DRIFT_HARD,
        )(refund_payment),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        wrapped(payment_id="pay_a", tenant_id="acme", request_id="dc-drift")
        with pytest.raises(DestructiveGrantError):
            wrapped(payment_id="pay_b", tenant_id="acme", request_id="dc-drift")


def test_crash_after_boundary_hard_blocks() -> None:
    store = get_or_store()
    grant = _issue(request_id="dc-crash", object_id="pay_1")
    events: list[str] = []

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        events.append("body")
        with side_effect():
            raise RuntimeError("boom")

    wrapped = apply_destructive_confirm(
        ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_binding())(
            refund_payment
        ),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        with pytest.raises(Exception):
            wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-crash")
        with pytest.raises(Exception):
            wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-crash")
    assert events == ["body"]


def test_storage_outage_fails_closed() -> None:
    class BoomStore(InMemoryDestructiveGrantStore):
        def try_consume(self, grant_id: str, request_id: str, now: float):
            raise RuntimeError("storage down")

    store = BoomStore()
    set_destructive_grant_store(store)
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        request_id="dc-out",
        expires_in=300,
        policy_version="test",
        store=store,
        bind_request_id=True,
    )

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        refund_payment, _policy(), tool_name="refund_payment", store=store
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        with pytest.raises(DestructiveGrantError) as exc:
            wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-out")
    assert exc.value.reason == "storage"


def test_file_store_atomic_consume(tmp_path: Any) -> None:
    store = FileDestructiveGrantStore(tmp_path / "grants.json")
    set_destructive_grant_store(store)
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        tenant="acme",
        expires_in=300,
        policy_version="test",
        store=store,
    )
    first = store.try_consume(grant.grant_id, "r1", grant.issued_at)
    second = store.try_consume(grant.grant_id, "r2", grant.issued_at)
    retry = store.try_consume(grant.grant_id, "r1", grant.issued_at)
    assert first.status == "allowed"
    assert second.status == "exhausted"
    assert retry.status == "retry"


def test_dict_grant_is_ignored() -> None:
    store = get_or_store()

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        refund_payment, _policy(), tool_name="refund_payment", store=store
    )
    fake = {
        "grant_id": "g",
        "operation": "refund",
        "object_type": "payment",
        "object_id": "pay_1",
    }
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(fake,))
    ):
        with pytest.raises(DestructiveGrantError, match="missing"):
            wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-dict")


def test_evidence_omits_payload() -> None:
    store = get_or_store()
    grant = _issue(request_id="dc-ev", object_id="pay_1")
    storage = InMemoryLedgerStorage()

    def refund_payment(payment_id: str, tenant_id: str, amount: str) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        ledger_sync(storage=storage, transition_binding=_binding())(refund_payment),
        _policy(),
        tool_name="refund_payment",
        store=store,
    )
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        wrapped(
            payment_id="pay_1",
            tenant_id="acme",
            amount="INTERNAL_REFUND_SECRET",
            request_id="dc-ev",
        )
    entry = storage.get("dc-ev")
    assert entry is not None
    dumped = json.dumps(entry.to_dict(), default=str)
    assert "INTERNAL_REFUND_SECRET" not in dumped
    _args, kwargs = sanitize_destructive_evidence(
        (),
        {"payment_id": "pay_1", "amount": "INTERNAL_REFUND_SECRET"},
    )
    assert kwargs["amount"] == PAYLOAD_OMITTED
    assert "INTERNAL_REFUND_SECRET" not in json.dumps(kwargs)


def test_sync_async_parity() -> None:
    store = get_or_store()
    grant = _issue(request_id="dc-async", object_id="pay_1")

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
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        assert (
            sync_wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-async")
            == "pay_1"
        )
    grant2 = _issue(request_id="dc-async2", object_id="pay_1")
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant2,))
    ):
        assert (
            asyncio.run(
                async_wrapped(
                    payment_id="pay_1", tenant_id="acme", request_id="dc-async2"
                )
            )
            == "pay_1"
        )


def test_omitted_config_preserves_behavior() -> None:
    cfg = load_config_from_string(
        """
tools:
  refund_payment:
    side_effect_class: irreversible
"""
    )
    assert cfg.destructive_confirm is None
    assert cfg.destructive_confirm_applies("refund_payment") is False

    def refund_payment(payment_id: str) -> str:
        return payment_id

    assert cfg.apply_tool("refund_payment", refund_payment) is refund_payment


def test_yaml_example_and_invalid_config() -> None:
    cfg = load_config_from_string(
        """
destructive_confirm:
  enabled: true
  missing_policy: error
  tools:
    refund_payment:
      operation: refund
      object:
        type: payment
        id_from: payment_id
      grant:
        bind_request_id: true
        max_uses: 1
        ttl_seconds: 300
    delete_file:
      operation: delete
      object:
        type: file
        id_from: file_id
      grant:
        bind_request_id: true
        max_uses: 1
        ttl_seconds: 120
tools:
  refund_payment:
    side_effect_class: irreversible
  delete_file:
    side_effect_class: irreversible
"""
    )
    assert cfg.destructive_confirm_applies("refund_payment") is True
    assert cfg.destructive_confirm_applies("search") is False

    with pytest.raises(ConfigError, match="unsupported"):
        load_config_from_string("destructive_confirm:\n  llm_approve: true\n")
    with pytest.raises(ConfigError, match="operation"):
        load_config_from_string(
            """
destructive_confirm:
  tools:
    refund_payment:
      object:
        type: payment
        id_from: payment_id
"""
        )
    with pytest.raises(ConfigError, match="ttl_seconds"):
        load_config_from_string(
            """
destructive_confirm:
  tools:
    refund_payment:
      operation: refund
      object: {type: payment, id_from: payment_id}
      grant: {ttl_seconds: 0}
"""
        )
    with pytest.raises(ConfigError, match="max_uses"):
        load_config_from_string(
            """
destructive_confirm:
  tools:
    refund_payment:
      operation: refund
      object: {type: payment, id_from: payment_id}
      grant: {max_uses: 0}
"""
        )


def test_production_rejects_memory_and_missing_irreversible() -> None:
    with pytest.raises(ConfigError, match="destructive_confirm.missing_policy"):
        load_config_from_string(
            """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [refund_payment]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
destructive_confirm:
  enabled: true
  missing_policy: warn
  storage: file
  path: ./grants.json
  tools:
    refund_payment:
      operation: refund
      object: {type: payment, id_from: payment_id}
tools:
  refund_payment:
    side_effect_class: irreversible
"""
        )
    with pytest.raises(ConfigError, match="memory"):
        load_config_from_string(
            """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [refund_payment]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
destructive_confirm:
  enabled: true
  storage: memory
  tools:
    refund_payment:
      operation: refund
      object: {type: payment, id_from: payment_id}
tools:
  refund_payment:
    side_effect_class: irreversible
"""
        )
    with pytest.raises(ConfigError, match="irreversible"):
        load_config_from_string(
            """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [purge_all]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
destructive_confirm:
  enabled: true
  storage: file
  path: ./grants.json
  tools: {}
tools:
  purge_all:
    side_effect_class: irreversible
"""
        )
    with pytest.raises(ConfigError, match="multi_node"):
        load_config_from_string(
            """
profile: production
deployment:
  topology: multi_node
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [refund_payment]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
destructive_confirm:
  enabled: true
  storage: file
  path: ./grants.json
  tools:
    refund_payment:
      operation: refund
      object: {type: payment, id_from: payment_id}
tools:
  refund_payment:
    side_effect_class: irreversible
"""
        )


def test_doctor_omitted_is_skip(tmp_path: Any) -> None:
    from mycelium import run_doctor

    path = tmp_path / "mycelium.yaml"
    path.write_text(
        """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [charge]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
tools:
  charge:
    side_effect_class: non_idempotent_mutate
""",
        encoding="utf-8",
    )
    report = run_doctor(path, connectivity=False)
    scanning = next(c for c in report.checks if c.id == "destructive.scanning")
    assert scanning.status == DoctorStatus.SKIP


def test_request_run_thread_binding() -> None:
    store = get_or_store()
    policy = DestructiveConfirmPolicy(
        enabled=True,
        policy_version="test",
        tools={
            "refund_payment": DestructiveToolPolicy(
                operation="refund",
                object=DestructiveObjectSpec(
                    object_type="payment", id_from="payment_id", tenant_from="tenant_id"
                ),
                grant=DestructiveGrantSpec(
                    bind_request_id=True,
                    bind_run_id=True,
                    bind_thread_id=True,
                    max_uses=1,
                    ttl_seconds=300,
                ),
            )
        },
    )
    grant = _issue(
        request_id="dc-bind",
        run_id="run-1",
        thread_id="thread-1",
        bind_request_id=True,
        bind_run_id=True,
        bind_thread_id=True,
    )

    def refund_payment(payment_id: str, tenant_id: str, **_: Any) -> str:
        return payment_id

    wrapped = apply_destructive_confirm(
        refund_payment, policy, tool_name="refund_payment", store=store
    )
    with execution_scope(
        TransitionScope(run_id="run-other", thread_id="thread-1", destructive_grants=(grant,))
    ):
        with pytest.raises(DestructiveGrantError):
            wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-bind")
    with execution_scope(
        TransitionScope(run_id="run-1", thread_id="thread-1", destructive_grants=(grant,))
    ):
        assert wrapped(payment_id="pay_1", tenant_id="acme", request_id="dc-bind") == "pay_1"


def test_enforce_sets_decision_on_deny() -> None:
    store = get_or_store()
    from mycelium.destructive_confirm import get_active_destructive_decision

    with pytest.raises(DestructiveGrantError):
        enforce_destructive_confirm(
            "refund_payment",
            (),
            {"payment_id": "pay_1", "tenant_id": "acme", "request_id": "x"},
            policy=_policy(),
            func=lambda payment_id, tenant_id: None,
            store=store,
        )
    decision = get_active_destructive_decision()
    assert decision is not None
    assert decision.decision in {"denied", "mismatched"}
    assert decision.reason == "missing"


def test_compatibility_with_loop_and_budget_omitted() -> None:
    cfg = load_config_from_string(
        """
destructive_confirm:
  enabled: true
  tools:
    refund_payment:
      operation: refund
      object: {type: payment, id_from: payment_id}
      grant: {bind_request_id: true, max_uses: 1, ttl_seconds: 60}
tools:
  refund_payment:
    side_effect_class: irreversible
"""
    )
    store = cfg.build_destructive_grant_store()
    set_destructive_grant_store(store)
    grant = issue_destructive_grant(
        operation="refund",
        object_type="payment",
        object_id="pay_1",
        request_id="dc-compat",
        expires_in=300,
        store=store,
        bind_request_id=True,
    )

    def refund_payment(payment_id: str, **_: Any) -> str:
        return payment_id

    wrapped = cfg.apply_tool("refund_payment", refund_payment)
    with execution_scope(
        TransitionScope(run_id="r", thread_id="t", destructive_grants=(grant,))
    ):
        assert wrapped(payment_id="pay_1", request_id="dc-compat") == "pay_1"
