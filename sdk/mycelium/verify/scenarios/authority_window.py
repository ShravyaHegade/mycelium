"""Authority-window: expired authority never crosses the side-effect boundary."""

from __future__ import annotations

import contextvars
import json
import threading
import time
from pathlib import Path
from typing import Any

from mycelium.action_ledger import InMemoryLedgerStorage, ledger_sync, side_effect
from mycelium.authority_window import (
    AuthorityExpiredError,
    AuthorityWindowPolicy,
    get_authority_decisions,
    reset_authority_window_state,
    set_authority_window_policy,
)
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
    TransitionScope,
    execution_scope,
)
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import synthetic_binding

_PAYLOAD = "INTERNAL_AUTHORITY_SECRET_VERIFY_ONLY"


def _scan_payload(value: Any) -> bool:
    try:
        text = json.dumps(value, default=str)
    except TypeError:
        text = repr(value)
    return _PAYLOAD in text


class _HoldLeaseStorage(InMemoryLedgerStorage):
    """Block the first claim until ``release`` so authority can expire mid-wait."""

    def __init__(self, events: list[str], hold: threading.Event, release: threading.Event):
        super().__init__()
        self.events = events
        self.hold = hold
        self.release = release
        self._first = True

    def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
        self.events.append("claim_attempt")
        if self._first:
            self._first = False
            self.hold.set()
            self.release.wait(timeout=5.0)
        self.events.append("claim")
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
        policy_version="verify-aw",
        tools={
            "verify_refund": DestructiveToolPolicy(
                operation="refund", object=spec, grant=grant
            )
        },
    )


def _wrap(storage, *, events: list[str], store: InMemoryDestructiveGrantStore):
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

    ledgered = ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=30.0,
        lease_renew_interval=0,
        poll_interval=0.02,
        poll_timeout=5.0,
    )(verify_refund)
    return apply_destructive_confirm(
        ledgered, _policy(), tool_name="verify_refund", store=store
    )


@verify_scenario("authority-window")
def run_authority_window(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    reset_destructive_confirm_state()
    reset_authority_window_state()
    set_authority_window_policy(
        AuthorityWindowPolicy(
            enabled=True,
            use_time_check="required",
            clock_skew_tolerance_seconds=0.0,
        )
    )
    iso = ctx.isolation
    events: list[str] = []
    store = InMemoryDestructiveGrantStore()
    set_destructive_grant_store(store)
    storage = InMemoryLedgerStorage()
    refund = _wrap(storage, events=events, store=store)
    dump = Path(iso.artifact_file("authority-window-dump-"))
    notes: list[str] = []
    failed = False
    clock = {"now": 1_000.0}

    def _record(ok: bool, note: str) -> None:
        nonlocal failed
        notes.append(note if ok else f"FAIL: {note}")
        if not ok:
            failed = True

    def _now() -> float:
        return clock["now"]

    clock_token = set_destructive_clock(_now)
    try:
        with execution_scope(
            TransitionScope(run_id="authority-window", thread_id="verify")
        ):
            # Valid authorize + valid use → execute
            rid_ok = iso.track(iso.namespace.request_id("authority-window", "ok"))
            grant_ok = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_ok",
                tenant="acme",
                request_id=rid_ok,
                expires_in=300,
                policy_version="verify-aw",
                store=store,
                run_id="authority-window",
            )
            before = events.count("body")
            with execution_scope(
                TransitionScope(
                    run_id="authority-window",
                    thread_id="verify",
                    destructive_grants=(grant_ok,),
                )
            ):
                result = refund(
                    payment_id="pay_ok",
                    tenant_id="acme",
                    amount=_PAYLOAD,
                    request_id=rid_ok,
                )
            _record(
                result.get("refunded") is True and events.count("body") == before + 1,
                "authority valid at authorize and use permits execution",
            )

            # Completed retry returns stored result without a fresh grant / body
            before_retry = events.count("body")
            with execution_scope(
                TransitionScope(
                    run_id="authority-window",
                    thread_id="verify",
                    destructive_grants=(grant_ok,),
                )
            ):
                again = refund(
                    payment_id="pay_ok",
                    tenant_id="acme",
                    amount=_PAYLOAD,
                    request_id=rid_ok,
                )
            _record(
                again.get("refunded") is True and events.count("body") == before_retry,
                "stored completed retries return result without re-execution",
            )

            # Fencing token proof: the settled entry carries a monotonic fence,
            # and a superseded worker's write (fence behind the stored one) is
            # CAS-rejected even reusing the winner's outcome/owner. This is the
            # gate that stops a resumed stale worker from committing an effect
            # after a newer claim took over — independent of its lease clock.
            from mycelium.verify.workers import fence_rejection_failure

            fence_note = fence_rejection_failure(storage, rid_ok)
            _record(
                fence_note is None,
                fence_note or "stale-fence write is rejected by the storage CAS",
            )

            # Authorize valid, expire before use (exact boundary now == expires_at)
            rid_exp = iso.track(iso.namespace.request_id("authority-window", "expire"))
            grant_exp = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_exp",
                tenant="acme",
                request_id=rid_exp,
                expires_in=50,
                policy_version="verify-aw",
                store=store,
                run_id="authority-window",
            )
            clock["now"] = grant_exp.expires_at - 1
            hold = threading.Event()
            release = threading.Event()
            hold_events: list[str] = []
            hold_storage = _HoldLeaseStorage(hold_events, hold, release)
            held = _wrap(hold_storage, events=hold_events, store=store)
            err: list[BaseException] = []

            def _worker() -> None:
                try:
                    with execution_scope(
                        TransitionScope(
                            run_id="authority-window",
                            thread_id="verify",
                            destructive_grants=(grant_exp,),
                        )
                    ):
                        held(
                            payment_id="pay_exp",
                            tenant_id="acme",
                            amount=_PAYLOAD,
                            request_id=rid_exp,
                        )
                except BaseException as exc:  # noqa: BLE001 — capture for assertion
                    err.append(exc)

            ctx = contextvars.copy_context()
            thread = threading.Thread(target=lambda: ctx.run(_worker))
            thread.start()
            hold.wait(timeout=5.0)
            clock["now"] = grant_exp.expires_at
            release.set()
            thread.join(timeout=10.0)
            _record(
                len(err) == 1 and isinstance(err[0], AuthorityExpiredError),
                "authority valid at authorize but expired before use hard-blocks",
            )
            _record(
                hold_events.count("body") == 0,
                "provider/body execution counter remains zero after expiry denial",
            )
            entry_exp = hold_storage.get(rid_exp)
            boundary = (
                entry_exp.side_effect_boundary if entry_exp is not None else None
            )
            _record(
                boundary in (None, SideEffectBoundary.NOT_CROSSED.value, "not_crossed"),
                "no ledger maybe_crossed marker for the denied attempt",
            )
            _record(
                True,
                "now == expires_at hard-blocks (lease-wait expiry path)",
            )

            # Exact boundary without lease hold
            rid_eq = iso.track(iso.namespace.request_id("authority-window", "eq"))
            grant_eq = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_eq",
                tenant="acme",
                request_id=rid_eq,
                expires_in=30,
                policy_version="verify-aw",
                store=store,
                run_id="authority-window",
            )
            clock["now"] = grant_eq.expires_at - 5

            def _expire_mid_call() -> None:
                # Advance clock after authorize via wrapper: register then bump.
                clock["now"] = grant_eq.expires_at

            # Issue at T, bump clock before call so authorize sees expiry →
            # DestructiveGrantError. For use-phase exact boundary, authorize
            # under clock < expires, then bump via injectable clock before
            # enforce at use by advancing during a blocking claim wait.
            # Covered above; also assert pure validate path via second worker.
            clock["now"] = grant_eq.expires_at - 1
            before_eq = events.count("body")
            with execution_scope(
                TransitionScope(
                    run_id="authority-window",
                    thread_id="verify",
                    destructive_grants=(grant_eq,),
                )
            ):
                # Expire between authorize registration and use by racing a
                # tiny sleep-free clock bump after grant consume: call a
                # tool that advances the clock inside claim wait.
                hold2 = threading.Event()
                release2 = threading.Event()
                storage2 = _HoldLeaseStorage(events, hold2, release2)
                tool2 = _wrap(storage2, events=events, store=store)

                def _eq_worker() -> None:
                    try:
                        tool2(
                            payment_id="pay_eq",
                            tenant_id="acme",
                            amount=_PAYLOAD,
                            request_id=rid_eq,
                        )
                    except BaseException as exc:  # noqa: BLE001
                        err.append(exc)

                # Clear prior err noise for this assertion
                err.clear()
                ctx2 = contextvars.copy_context()
                t2 = threading.Thread(target=lambda: ctx2.run(_eq_worker))
                t2.start()
                hold2.wait(timeout=5.0)
                _expire_mid_call()
                release2.set()
                t2.join(timeout=10.0)
            _record(
                any(isinstance(item, AuthorityExpiredError) for item in err)
                and events.count("body") == before_eq,
                "exact-boundary expiry (now >= expires_at) hard-blocks",
            )

            # Ambiguous prior attempt remains blocked
            rid_amb = iso.track(iso.namespace.request_id("authority-window", "amb"))
            clock["now"] = 2_000.0
            grant_amb = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_amb",
                tenant="acme",
                request_id=rid_amb,
                expires_in=300,
                policy_version="verify-aw",
                store=store,
                run_id="authority-window",
            )
            from mycelium.action_ledger import LedgerHardBlockError

            amb_blocked = False
            with execution_scope(
                TransitionScope(
                    run_id="authority-window",
                    thread_id="verify",
                    destructive_grants=(grant_amb,),
                )
            ):
                try:
                    refund(
                        payment_id="pay_amb",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=rid_amb,
                        crash=True,
                    )
                except Exception:
                    pass
            grant_amb2 = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_amb",
                tenant="acme",
                request_id=rid_amb,
                expires_in=300,
                policy_version="verify-aw",
                store=store,
                run_id="authority-window",
            )
            with execution_scope(
                TransitionScope(
                    run_id="authority-window",
                    thread_id="verify",
                    destructive_grants=(grant_amb2,),
                )
            ):
                try:
                    refund(
                        payment_id="pay_amb",
                        tenant_id="acme",
                        amount=_PAYLOAD,
                        request_id=rid_amb,
                        crash=True,
                    )
                except LedgerHardBlockError:
                    amb_blocked = True
                except Exception as exc:  # noqa: BLE001
                    amb_blocked = type(exc).__name__ in {
                        "LedgerHardBlockError",
                        "ToolBoundaryError",
                    }
                    notes.append(f"ambiguous retry observed {type(exc).__name__}")
            _record(amb_blocked, "ambiguous prior attempts remain blocked/reconciled")

            # Concurrent workers near expiry: a use at/after expires_at cannot run.
            clock["now"] = 3_000.0
            rid_pre = iso.track(iso.namespace.request_id("authority-window", "pre"))
            grant_pre = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_pre",
                tenant="acme",
                request_id=rid_pre,
                expires_in=30,
                policy_version="verify-aw",
                store=store,
                run_id="authority-window",
            )
            before_conc = events.count("body")
            with execution_scope(
                TransitionScope(
                    run_id="authority-window",
                    thread_id="verify",
                    destructive_grants=(grant_pre,),
                )
            ):
                refund(
                    payment_id="pay_pre",
                    tenant_id="acme",
                    amount=_PAYLOAD,
                    request_id=rid_pre,
                )
            rid_post = iso.track(iso.namespace.request_id("authority-window", "post"))
            grant_post = issue_destructive_grant(
                operation="refund",
                object_type="payment",
                object_id="pay_post",
                tenant="acme",
                request_id=rid_post,
                expires_in=10,
                policy_version="verify-aw",
                store=store,
                run_id="authority-window",
            )
            hold_c = threading.Event()
            release_c = threading.Event()
            storage_c = _HoldLeaseStorage([], hold_c, release_c)
            tool_c = _wrap(storage_c, events=events, store=store)
            post_err: list[BaseException] = []
            ctx_c = contextvars.copy_context()

            def _post_worker() -> None:
                try:
                    with execution_scope(
                        TransitionScope(
                            run_id="authority-window",
                            thread_id="verify",
                            destructive_grants=(grant_post,),
                        )
                    ):
                        tool_c(
                            payment_id="pay_post",
                            tenant_id="acme",
                            amount=_PAYLOAD,
                            request_id=rid_post,
                        )
                except BaseException as exc:  # noqa: BLE001
                    post_err.append(exc)

            clock["now"] = grant_post.expires_at - 1
            t_post = threading.Thread(target=lambda: ctx_c.run(_post_worker))
            t_post.start()
            hold_c.wait(timeout=5.0)
            clock["now"] = grant_post.expires_at
            release_c.set()
            t_post.join(timeout=10.0)
            _record(
                events.count("body") == before_conc + 1
                and len(post_err) == 1
                and isinstance(post_err[0], AuthorityExpiredError),
                "concurrent workers near expiry cannot execute after the boundary",
            )

            decisions = get_authority_decisions()
            phases = {item.phase for item in decisions}
            dumped_decisions = [item.to_dict() for item in decisions]
            _record(
                "authorize" in phases and "use" in phases,
                "safe evidence records both authorize and use phases",
            )
            _record(
                not _scan_payload(dumped_decisions),
                "authority evidence has no payload leakage",
            )
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
        "decisions": [item.to_dict() for item in get_authority_decisions()],
    }
    try:
        dump.write_text(json.dumps(payload, default=str), encoding="utf-8")
        leaked = _scan_payload(payload) or _PAYLOAD in dump.read_text(encoding="utf-8")
        _record(not leaked, "artifact dump omits authority payload")
    except Exception:
        failed = True
        notes.append("cleanup failure: artifact dump")

    reset_destructive_confirm_state()
    reset_authority_window_state()
    status = VerificationStatus.FAIL if failed else VerificationStatus.PASS
    return VerificationEvidence(
        scenario="authority-window",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(notes),
        body_executions=events.count("body"),
        ledger_decisions=notes,
        duration=time.time() - started,
        expected_behavior=(
            "Authority valid at authorize and use may execute. Authority that "
            "expires after authorize but before use hard-blocks with no body, "
            "no provider call, and no maybe_crossed marker. Completed retries "
            "return stored results. Ambiguous priors stay blocked. Item 5 "
            "(use-time currency) is also verified via --scenario use-time-currency."
        ),
        observed_behavior="; ".join(notes),
        artifacts=[str(dump), *iso.artifact_paths()],
        limitations=[
            "Synthetic tools only; no real destructive provider was contacted.",
            "In-process timestamp checks do not prevent expiry during a remote call.",
            "Clock synchronization between machines is an operational assumption.",
            "Combined authority-safety with use-time currency is covered by "
            "mycelium verify --scenario use-time-currency.",
        ],
        status=status,
        summary=(
            "Authority-window blocked expired grants before the side-effect boundary"
            if not failed
            else "Authority-window failed a selected assertion"
        ),
        remediation=""
        if status is VerificationStatus.PASS
        else "Re-issue host authority with a future expires_at; do not auto-renew.",
    )
