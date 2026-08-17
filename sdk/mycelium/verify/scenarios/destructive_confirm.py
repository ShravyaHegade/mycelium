"""Destructive-confirm: ungranted objects never reach claim or body."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

from mycelium.action_ledger import InMemoryLedgerStorage, ledger_sync, side_effect
from mycelium.destructive_confirm import (
    DestructiveConfirmPolicy,
    DestructiveGrantError,
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
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import synthetic_binding

_PAYLOAD = "INTERNAL_REFUND_SECRET_VERIFY_ONLY"


def _scan_payload(value: Any) -> bool:
    try:
        text = json.dumps(value, default=str)
    except TypeError:
        text = repr(value)
    return _PAYLOAD in text


class _RecordingStorage(InMemoryLedgerStorage):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
        self.events.append("claim")
        if _scan_payload(entry.to_dict() if hasattr(entry, "to_dict") else entry):
            raise RuntimeError("storage refused a payload that contained secrets")
        return super().try_claim_inflight(entry, lease_ttl=lease_ttl)


def _policy() -> DestructiveConfirmPolicy:
    spec = DestructiveObjectSpec(
        object_type="payment",
        id_from="payment_id",
        tenant_from="tenant_id",
    )
    grant = DestructiveGrantSpec(bind_request_id=True, max_uses=1, ttl_seconds=300)
    return DestructiveConfirmPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="verify",
        tools={
            "verify_refund": DestructiveToolPolicy(
                operation="refund", object=spec, grant=grant
            ),
            "verify_cancel": DestructiveToolPolicy(
                operation="cancel", object=spec, grant=grant
            ),
        },
    )


def _wrap(storage, *, tool: str, events: list[str], store: InMemoryDestructiveGrantStore):
    binding = synthetic_binding()

    def verify_refund(
        payment_id: str,
        tenant_id: str,
        amount: str,
        crash: bool = False,
    ) -> dict[str, Any]:
        events.append("body")
        if crash:
            with side_effect():
                raise RuntimeError("verify crash after possible boundary")
        return {"refunded": True, "payment_id": payment_id, "amount": amount}

    def verify_cancel(
        payment_id: str,
        tenant_id: str,
        amount: str,
    ) -> dict[str, Any]:
        events.append("body")
        return {"cancelled": True, "payment_id": payment_id}

    target = verify_refund if tool == "verify_refund" else verify_cancel
    ledgered = ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=30.0,
        lease_renew_interval=0,
        poll_interval=0.02,
        poll_timeout=5.0,
    )(target)
    return apply_destructive_confirm(
        ledgered, _policy(), tool_name=tool, store=store
    )


@verify_scenario("destructive-confirm")
def run_destructive_confirm(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    reset_destructive_confirm_state()
    iso = ctx.isolation
    events: list[str] = []
    store = InMemoryDestructiveGrantStore()
    set_destructive_grant_store(store)
    storage = _RecordingStorage(events)
    refund = _wrap(storage, tool="verify_refund", events=events, store=store)
    cancel = _wrap(storage, tool="verify_cancel", events=events, store=store)
    dump = Path(iso.artifact_file("destructive-confirm-dump-"))
    notes: list[str] = []
    failed = False
    clock = {"now": 1_000.0}

    def _record(ok: bool, note: str) -> None:
        nonlocal failed
        notes.append(note)
        if not ok:
            failed = True

    def _now() -> float:
        return clock["now"]

    clock_token = set_destructive_clock(_now)
    try:
        with execution_scope(
            TransitionScope(run_id="destructive-confirm", thread_id="verify")
        ):
            before = list(events)
            rid_missing = iso.track(iso.namespace.request_id("destructive-confirm", "missing"))
            try:
                refund(
                    payment_id="pay_1",
                    tenant_id="acme",
                    amount=_PAYLOAD,
                    request_id=rid_missing,
                )
                _record(False, "missing grant executed")
            except DestructiveGrantError:
                _record(events == before, "missing grant blocked before claim")

            grant_refund = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_1",
                request_id=iso.track(iso.namespace.request_id("destructive-confirm", "wrong-op")),
                tenant="acme",
                expires_in=300,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            before = list(events)
            try:
                with execution_scope(
                    TransitionScope(
                        run_id="destructive-confirm",
                        thread_id="verify",
                        destructive_grants=(grant_refund,),
                    )
                ):
                    cancel(
                        payment_id="pay_1",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=grant_refund.request_id,
                    )
                _record(False, "wrong operation executed")
            except DestructiveGrantError:
                _record(events == before, "wrong operation blocked")

            rid_obj = iso.track(iso.namespace.request_id("destructive-confirm", "wrong-obj"))
            grant_obj = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_1",
                request_id=rid_obj,
                tenant="acme",
                expires_in=300,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            before = list(events)
            try:
                with execution_scope(
                    TransitionScope(
                        run_id="destructive-confirm",
                        thread_id="verify",
                        destructive_grants=(grant_obj,),
                    )
                ):
                    refund(
                        payment_id="pay_2",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=rid_obj,
                    )
                _record(False, "wrong object executed")
            except DestructiveGrantError:
                _record(events == before, "wrong object blocked")

            rid_tenant = iso.track(iso.namespace.request_id("destructive-confirm", "tenant"))
            grant_tenant = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_1",
                request_id=rid_tenant,
                tenant="acme",
                expires_in=300,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            before = list(events)
            try:
                with execution_scope(
                    TransitionScope(
                        run_id="destructive-confirm",
                        thread_id="verify",
                        destructive_grants=(grant_tenant,),
                    )
                ):
                    refund(
                        payment_id="pay_1",
                        tenant_id="other",
                        amount=_PAYLOAD,
                        request_id=rid_tenant,
                    )
                _record(False, "wrong tenant executed")
            except DestructiveGrantError:
                _record(events == before, "wrong tenant blocked")

            rid_exp = iso.track(iso.namespace.request_id("destructive-confirm", "expired"))
            grant_exp = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_1",
                request_id=rid_exp,
                tenant="acme",
                expires_in=10,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            clock["now"] = grant_exp.expires_at
            before = list(events)
            try:
                with execution_scope(
                    TransitionScope(
                        run_id="destructive-confirm",
                        thread_id="verify",
                        destructive_grants=(grant_exp,),
                    )
                ):
                    refund(
                        payment_id="pay_1",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=rid_exp,
                    )
                _record(False, "expired grant executed")
            except DestructiveGrantError:
                _record(events == before, "expired grant blocked")
            clock["now"] = 1_000.0

            rid_ok = iso.track(iso.namespace.request_id("destructive-confirm", "allow"))
            grant_ok = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_ok",
                request_id=rid_ok,
                tenant="acme",
                expires_in=300,
                max_uses=1,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            with execution_scope(
                TransitionScope(
                    run_id="destructive-confirm",
                    thread_id="verify",
                    destructive_grants=(grant_ok,),
                )
            ):
                refund(
                    payment_id="pay_ok",
                    tenant_id="acme",
                    amount=_PAYLOAD,
                    request_id=rid_ok,
                )
                refund(
                    payment_id="pay_ok",
                    tenant_id="acme",
                    amount=_PAYLOAD,
                    request_id=rid_ok,
                )
            _record(events.count("body") == 1, "valid grant once; retry reused")
            rec = store.get(grant_ok.grant_id)
            _record(
                rec is not None and rec.get("uses_remaining") == 0,
                "retry did not consume a second use",
            )

            rid_exh = iso.track(iso.namespace.request_id("destructive-confirm", "exhausted"))
            grant_exh = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_exh",
                request_id=rid_exh,
                tenant="acme",
                expires_in=300,
                max_uses=1,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            store.try_consume(grant_exh.grant_id, "other-request", clock["now"])
            before = list(events)
            try:
                with execution_scope(
                    TransitionScope(
                        run_id="destructive-confirm",
                        thread_id="verify",
                        destructive_grants=(grant_exh,),
                    )
                ):
                    refund(
                        payment_id="pay_exh",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=rid_exh,
                    )
                _record(False, "exhausted grant executed")
            except DestructiveGrantError:
                _record(events == before, "exhausted grant blocked")

            rid_drift = iso.track(iso.namespace.request_id("destructive-confirm", "drift"))
            grant_drift = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_a",
                request_id=rid_drift,
                tenant="acme",
                expires_in=300,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            before = list(events)
            try:
                with execution_scope(
                    TransitionScope(
                        run_id="destructive-confirm",
                        thread_id="verify",
                        destructive_grants=(grant_drift,),
                    )
                ):
                    refund(
                        payment_id="pay_b",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=rid_drift,
                    )
                _record(False, "changed target executed")
            except DestructiveGrantError:
                _record(events == before, "changed target failed closed")

            rid_crash = iso.track(iso.namespace.request_id("destructive-confirm", "crash"))
            grant_crash = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_crash",
                request_id=rid_crash,
                tenant="acme",
                expires_in=300,
                policy_version="verify",
                store=store,
                bind_request_id=True,
            )
            with execution_scope(
                TransitionScope(
                    run_id="destructive-confirm",
                    thread_id="verify",
                    destructive_grants=(grant_crash,),
                )
            ):
                try:
                    refund(
                        payment_id="pay_crash",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        crash=True,
                        request_id=rid_crash,
                    )
                    _record(False, "crash call returned")
                except Exception:
                    pass
                bodies = events.count("body")
                try:
                    refund(
                        payment_id="pay_crash",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=rid_crash,
                    )
                    _record(False, "crash retry reused grant for a fresh body")
                except Exception:
                    _record(
                        events.count("body") == bodies,
                        "crash after boundary hard-blocked rather than reuse",
                    )

            hits = {"body": 0}
            hits_lock = threading.Lock()
            barrier = threading.Barrier(2)
            rid_c1 = iso.track(iso.namespace.request_id("destructive-confirm", "c1"))
            rid_c2 = iso.track(iso.namespace.request_id("destructive-confirm", "c2"))
            grant_c = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_conc",
                tenant="acme",
                expires_in=300,
                max_uses=1,
                policy_version="verify",
                store=store,
            )
            concurrent_policy = DestructiveConfirmPolicy(
                enabled=True,
                missing_policy="error",
                policy_version="verify",
                tools={
                    "verify_refund": DestructiveToolPolicy(
                        operation="refund",
                        object=DestructiveObjectSpec(
                            object_type="payment",
                            id_from="payment_id",
                            tenant_from="tenant_id",
                        ),
                        grant=DestructiveGrantSpec(max_uses=1, ttl_seconds=300),
                    )
                },
            )

            def _worker(request_id: str) -> None:
                set_destructive_clock(_now)

                def verify_refund(payment_id: str, tenant_id: str, amount: str, **_: Any) -> str:
                    with hits_lock:
                        hits["body"] += 1
                    return payment_id

                wrapped = apply_destructive_confirm(
                    verify_refund,
                    concurrent_policy,
                    tool_name="verify_refund",
                    store=store,
                )
                barrier.wait()
                try:
                    with execution_scope(
                        TransitionScope(
                            run_id="destructive-confirm",
                            thread_id="verify",
                            destructive_grants=(grant_c,),
                        )
                    ):
                        wrapped(
                            payment_id="pay_conc",
                            tenant_id="acme",
                            amount=_PAYLOAD,
                            request_id=request_id,
                        )
                except DestructiveGrantError:
                    return

            threads = [
                threading.Thread(target=_worker, args=(rid_c1,)),
                threading.Thread(target=_worker, args=(rid_c2,)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            _record(hits["body"] == 1, "concurrent workers consumed one-use grant once")
    finally:
        try:
            from mycelium.destructive_confirm import reset_destructive_clock

            reset_destructive_clock(clock_token)
        except Exception:
            failed = True
            notes.append("cleanup failure: clock reset")

    payload = {
        "events": events,
        "notes": notes,
        "entries": [entry.to_dict() for entry in storage._entries.values()]
        if hasattr(storage, "_entries")
        else [],
    }
    try:
        dump.write_text(json.dumps(payload, default=str), encoding="utf-8")
        leaked = _scan_payload(payload) or _PAYLOAD in dump.read_text(encoding="utf-8")
        _record(not leaked, "evidence omitted destructive payload")
    except Exception:
        failed = True
        notes.append("cleanup failure: artifact dump")

    reset_destructive_confirm_state()
    status = VerificationStatus.FAIL if failed else VerificationStatus.PASS
    return VerificationEvidence(
        scenario="destructive-confirm",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(notes),
        body_executions=events.count("body"),
        ledger_decisions=notes,
        duration=time.time() - started,
        expected_behavior=(
            "Missing, mismatched, expired, or exhausted grants fail closed "
            "before claim. One exact grant permits one execution; retries "
            "reuse the ledger result. Concurrent workers cannot consume a "
            "one-use grant twice. Evidence omits the payload."
        ),
        observed_behavior="; ".join(notes),
        artifacts=[str(dump), *iso.artifact_paths()],
        limitations=[
            "Synthetic tools only; no real destructive provider was contacted.",
            "Grants must be minted by the host; doctor cannot prove call sites.",
            "A grant authorizes an attempt, not the provider's final outcome.",
        ],
        status=status,
        summary=(
            "Destructive confirmation blocked ungranted objects before claim"
            if not failed
            else "Destructive confirmation failed a selected assertion"
        ),
        remediation=""
        if status is VerificationStatus.PASS
        else "Issue an exact host grant; tool permission is not object authorization.",
    )
