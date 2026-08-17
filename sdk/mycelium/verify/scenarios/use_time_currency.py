"""Use-time currency: stale/changed decide-time facts never cross the boundary."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mycelium.action_ledger import InMemoryLedgerStorage, ledger_sync, side_effect
from mycelium.transition import SideEffectBoundary, TransitionScope, execution_scope
from mycelium.use_time_currency import (
    UseTimeCurrencyError,
    UseTimeCurrencyPolicy,
    UseTimeFactSpec,
    UseTimeToolPolicy,
    ValidatorResult,
    apply_use_time_currency,
    register_use_time_validator,
    reset_use_time_currency_state,
    set_use_time_clock,
    use_time_facts,
)
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import synthetic_binding

_PAYLOAD = "INTERNAL_USE_TIME_SECRET_VERIFY_ONLY"


def _scan_payload(value: Any) -> bool:
    try:
        text = json.dumps(value, default=str)
    except TypeError:
        text = repr(value)
    return _PAYLOAD in text


class _HoldLeaseStorage(InMemoryLedgerStorage):
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


def _policy() -> UseTimeCurrencyPolicy:
    return UseTimeCurrencyPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="verify-utc",
        tools={
            "verify_refund": UseTimeToolPolicy(
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


def _wrap(storage, *, events: list[str], state: dict[str, Any]):
    binding = synthetic_binding()

    def payment_state(*, fact, subject_id, **_kwargs):
        current = state.get(str(subject_id), {})
        return ValidatorResult(
            current=bool(current.get("refundable", False)),
            revision=str(current.get("version", "")),
            value=bool(current.get("refundable", False)),
            reason="valid" if current.get("refundable") else "condition_false",
        )

    register_use_time_validator("payment_state", payment_state)

    def verify_refund(
        payment_id: str,
        payment_version: str,
        amount: str = "10",
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
    return apply_use_time_currency(
        ledgered, _policy(), tool_name="verify_refund"
    )


@verify_scenario("use-time-currency")
def run_use_time_currency(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    reset_use_time_currency_state()
    iso = ctx.isolation
    events: list[str] = []
    state: dict[str, Any] = {
        "pay_1": {"refundable": True, "version": "1"},
        "pay_stale": {"refundable": True, "version": "1"},
        "pay_change": {"refundable": True, "version": "1"},
    }
    storage = InMemoryLedgerStorage()
    refund = _wrap(storage, events=events, state=state)
    dump = Path(iso.artifact_file("use-time-currency-dump-"))
    notes: list[str] = []
    failed = False
    clock = {"now": 1_000.0}
    set_use_time_clock(lambda: clock["now"])

    def _record(ok: bool, note: str) -> None:
        nonlocal failed
        notes.append(note if ok else f"FAIL: {note}")
        if not ok:
            failed = True

    with execution_scope(TransitionScope(run_id="use-time-currency", thread_id="verify")):
        # Current fact permits execution.
        rid_ok = iso.track(iso.namespace.request_id("use-time-currency", "ok"))
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_1",
            value=True,
            revision="1",
            max_age_seconds=30,
            request_id=rid_ok,
            require_value=True,
        )
        events.clear()
        out = refund(
            payment_id="pay_1",
            payment_version="1",
            amount="10",
            request_id=rid_ok,
            run_id="use-time-currency",
        )
        _record(out.get("refunded") is True and "body" in events, "current fact allows body")

        # Decide-time true, use-time false blocks.
        rid_false = iso.track(iso.namespace.request_id("use-time-currency", "false"))
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_change",
            value=True,
            revision="1",
            max_age_seconds=30,
            request_id=rid_false,
            require_value=True,
        )
        state["pay_change"] = {"refundable": False, "version": "1"}
        events.clear()
        err: list[BaseException] = []
        try:
            refund(
                payment_id="pay_change",
                payment_version="1",
                request_id=rid_false,
                run_id="use-time-currency",
            )
        except UseTimeCurrencyError as exc:
            err.append(exc)
        entry = storage.get(rid_false)
        _record(
            len(err) == 1
            and err[0].reason == "condition_false"
            and "body" not in events
            and (
                entry is None
                or entry.side_effect_boundary != SideEffectBoundary.MAYBE_CROSSED
            ),
            "use-time false blocks without maybe_crossed",
        )

        # Revision mismatch.
        rid_rev = iso.track(iso.namespace.request_id("use-time-currency", "rev"))
        state["pay_1"] = {"refundable": True, "version": "2"}
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_1",
            value=True,
            revision="1",
            max_age_seconds=30,
            request_id=rid_rev,
            require_value=True,
        )
        events.clear()
        err.clear()
        try:
            refund(
                payment_id="pay_1",
                payment_version="1",
                request_id=rid_rev,
                run_id="use-time-currency",
            )
        except UseTimeCurrencyError as exc:
            err.append(exc)
        _record(
            len(err) == 1
            and err[0].reason == "revision_mismatch"
            and "body" not in events,
            "revision mismatch blocks",
        )
        state["pay_1"] = {"refundable": True, "version": "1"}

        # Max-age stale.
        rid_age = iso.track(iso.namespace.request_id("use-time-currency", "age"))
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_stale",
            value=True,
            revision="1",
            max_age_seconds=30,
            request_id=rid_age,
            require_value=True,
            observed_at=datetime.fromtimestamp(
                clock["now"], tz=timezone.utc
            ),
        )
        clock["now"] = 1_040.0
        events.clear()
        err.clear()
        try:
            refund(
                payment_id="pay_stale",
                payment_version="1",
                request_id=rid_age,
                run_id="use-time-currency",
            )
        except UseTimeCurrencyError as exc:
            err.append(exc)
        _record(
            len(err) == 1 and err[0].reason == "stale" and "body" not in events,
            "max_age stale blocks",
        )
        clock["now"] = 1_000.0

        # Missing validator.
        rid_missing = iso.track(iso.namespace.request_id("use-time-currency", "missing-v"))
        reset_use_time_currency_state()
        set_use_time_clock(lambda: clock["now"])
        refund2 = _wrap(InMemoryLedgerStorage(), events=events, state=state)
        # Drop validator registration after wrap rebuilt it — clear and re-apply.
        from mycelium.use_time_currency import _validators

        _validators.pop("payment_state", None)
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_1",
            value=True,
            revision="1",
            max_age_seconds=30,
            request_id=rid_missing,
            require_value=True,
        )
        events.clear()
        err.clear()
        try:
            refund2(
                payment_id="pay_1",
                payment_version="1",
                request_id=rid_missing,
                run_id="use-time-currency",
            )
        except UseTimeCurrencyError as exc:
            err.append(exc)
        _record(
            len(err) == 1
            and err[0].reason == "validator_missing"
            and "body" not in events,
            "missing validator blocks",
        )

        # Fact change during lease wait.
        reset_use_time_currency_state()
        set_use_time_clock(lambda: clock["now"])
        events.clear()
        hold = threading.Event()
        release = threading.Event()
        hold_storage = _HoldLeaseStorage(events, hold, release)
        state["pay_lease"] = {"refundable": True, "version": "1"}
        refund3 = _wrap(hold_storage, events=events, state=state)
        rid_lease = iso.track(iso.namespace.request_id("use-time-currency", "lease"))
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_lease",
            value=True,
            revision="1",
            max_age_seconds=30,
            request_id=rid_lease,
            require_value=True,
        )
        err.clear()

        def _run() -> None:
            try:
                refund3(
                    payment_id="pay_lease",
                    payment_version="1",
                    request_id=rid_lease,
                    run_id="use-time-currency",
                )
            except UseTimeCurrencyError as exc:
                err.append(exc)

        thread = threading.Thread(target=_run)
        thread.start()
        hold.wait(timeout=5.0)
        state["pay_lease"] = {"refundable": False, "version": "1"}
        release.set()
        thread.join(timeout=5.0)
        _record(
            len(err) == 1 and "body" not in events,
            "fact change during lease wait blocks",
        )

        # Completed RETURN without revalidation body.
        reset_use_time_currency_state()
        set_use_time_clock(lambda: clock["now"])
        storage4 = InMemoryLedgerStorage()
        state["pay_ret"] = {"refundable": True, "version": "1"}
        refund4 = _wrap(storage4, events=events, state=state)
        rid_ret = iso.track(iso.namespace.request_id("use-time-currency", "return"))
        use_time_facts.capture(
            name="payment.refundable",
            subject_type="payment",
            subject_id="pay_ret",
            value=True,
            revision="1",
            max_age_seconds=30,
            request_id=rid_ret,
            require_value=True,
        )
        events.clear()
        first = refund4(
            payment_id="pay_ret",
            payment_version="1",
            request_id=rid_ret,
            run_id="use-time-currency",
        )
        state["pay_ret"] = {"refundable": False, "version": "9"}
        events.clear()
        second = refund4(
            payment_id="pay_ret",
            payment_version="1",
            request_id=rid_ret,
            run_id="use-time-currency",
        )
        _record(
            first == second and "body" not in events,
            "completed RETURN skips body and use revalidation",
        )

    dump.write_text(json.dumps({"notes": notes, "events": events}, indent=2), encoding="utf-8")
    leaked = _scan_payload(notes) or _scan_payload(events)
    reset_use_time_currency_state()

    status = VerificationStatus.FAIL if failed or leaked else VerificationStatus.PASS
    return VerificationEvidence(
        scenario="use-time-currency",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(notes),
        body_executions=events.count("body"),
        ledger_decisions=notes,
        duration=time.time() - started,
        expected_behavior=(
            "Current facts may execute. Stale, changed, missing, or unverifiable "
            "facts hard-block with no body and no maybe_crossed. Completed RETURN "
            "skips use revalidation. Authority-window and use-time currency together "
            "complete the batch guarantee."
        ),
        observed_behavior="; ".join(notes),
        artifacts=[str(dump), *iso.artifact_paths()],
        limitations=[
            "Synthetic tools and fake fact stores only; no real provider.",
            "Local revalidation cannot eliminate a fact change during a remote call.",
            "Validator correctness and replica consistency are host responsibilities.",
        ],
        status=status,
        summary=(
            "use-time currency blocks stale/changed facts"
            if status == VerificationStatus.PASS
            else "use-time currency scenario failed"
        ),
        remediation=""
        if status == VerificationStatus.PASS
        else "Inspect use-time-currency-dump artifact.",
    )
