"""Entity-guard: unauthorized destinations never reach claim or evidence."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from mycelium.action_ledger import InMemoryLedgerStorage, ledger_sync
from mycelium.entity_guard import (
    DEST_EMAIL,
    DEST_HTTPS_URL,
    DestinationAllow,
    DestinationSpec,
    EntityGuardError,
    EntityGuardPolicy,
    ToolDestinationPolicy,
    apply_entity_guard,
    reset_entity_guard_state,
)
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import synthetic_binding

_PAYLOAD = "INTERNAL_PAYROLL_SSN_VERIFY_ONLY"
_FORBIDDEN = "exfil@evil.example"


def _scan_payload(value: Any) -> bool:
    try:
        text = json.dumps(value, default=str)
    except TypeError:
        text = repr(value)
    return _PAYLOAD in text or _FORBIDDEN in text


class _RecordingStorage(InMemoryLedgerStorage):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
        self.events.append("claim")
        if _scan_payload(entry.to_dict() if hasattr(entry, "to_dict") else entry):
            raise RuntimeError("storage refused a payload that contained secrets")
        return super().try_claim_inflight(entry, lease_ttl=lease_ttl)


def _policy() -> EntityGuardPolicy:
    return EntityGuardPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="verify",
        tools={
            "verify_send": ToolDestinationPolicy(
                destinations=(
                    DestinationSpec(
                        path="recipient",
                        dest_type=DEST_EMAIL,
                        allow=DestinationAllow(
                            addresses=frozenset({"billing@customer.com"}),
                            domains=frozenset({"customer.com"}),
                        ),
                    ),
                    DestinationSpec(
                        path="cc",
                        dest_type=DEST_EMAIL,
                        allow=DestinationAllow(),
                        required=False,
                    ),
                )
            ),
            "verify_post": ToolDestinationPolicy(
                destinations=(
                    DestinationSpec(
                        path="url",
                        dest_type=DEST_HTTPS_URL,
                        allow=DestinationAllow(hosts=frozenset({"api.stripe.com"})),
                        reject_redirects=True,
                    ),
                )
            ),
        },
    )


def _wrap(storage, *, tool: str, events: list[str], seen: list[Any]):
    binding = synthetic_binding()

    def verify_send(
        recipient: str,
        body: str,
        cc: list[str] | None = None,
    ) -> dict[str, Any]:
        events.append("body")
        seen.append(recipient)
        return {"sent": True, "recipient": recipient, "body": body, "cc": cc}

    def verify_post(url: str, body: str) -> dict[str, Any]:
        events.append("body")
        seen.append(url)
        return {"posted": True, "url": url, "body": body}

    target = verify_send if tool == "verify_send" else verify_post
    ledgered = ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=30.0,
        lease_renew_interval=0,
        poll_interval=0.02,
        poll_timeout=5.0,
    )(target)
    return apply_entity_guard(ledgered, _policy(), tool_name=tool)


@verify_scenario("entity-guard")
def run_entity_guard(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    reset_entity_guard_state()
    iso = ctx.isolation
    events: list[str] = []
    seen: list[Any] = []
    storage = _RecordingStorage(events)
    send = _wrap(storage, tool="verify_send", events=events, seen=seen)
    post = _wrap(storage, tool="verify_post", events=events, seen=seen)
    dump = Path(iso.artifact_file("entity-guard-dump-"))
    notes: list[str] = []
    failed = False

    def _record(ok: bool, note: str) -> None:
        nonlocal failed
        notes.append(note)
        if not ok:
            failed = True

    with execution_scope(TransitionScope(run_id="entity-guard", thread_id="verify")):
        rid_ok = iso.track(iso.namespace.request_id("entity-guard", "allow"))
        send(recipient="billing@customer.com", body=_PAYLOAD, request_id=rid_ok)
        _record("body" in events and "claim" in events, "allowed destination claimed")

        before = list(events)
        rid_deny = iso.track(iso.namespace.request_id("entity-guard", "deny"))
        try:
            send(recipient=_FORBIDDEN, body=_PAYLOAD, request_id=rid_deny)
            _record(False, "forbidden recipient executed")
        except EntityGuardError as exc:
            _record(
                exc.reason == "not_allowed" and events == before,
                "forbidden recipient blocked before claim",
            )

        before = list(events)
        rid_cc = iso.track(iso.namespace.request_id("entity-guard", "cc"))
        try:
            send(
                recipient="billing@customer.com",
                body=_PAYLOAD,
                cc=[_FORBIDDEN],
                request_id=rid_cc,
            )
            _record(False, "forbidden cc executed")
        except EntityGuardError:
            _record(events == before, "forbidden cc blocked before claim")

        before = list(events)
        rid_url = iso.track(iso.namespace.request_id("entity-guard", "url"))
        try:
            post(
                url="https://api.stripe.com.evil.example/v1",
                body=_PAYLOAD,
                request_id=rid_url,
            )
            _record(False, "lookalike host executed")
        except EntityGuardError:
            _record(events == before, "lookalike host blocked before claim")

        before = list(events)
        rid_redir = iso.track(iso.namespace.request_id("entity-guard", "redir"))
        try:
            post(
                url="https://api.stripe.com/redirect?next=https://evil.example",
                body=_PAYLOAD,
                request_id=rid_redir,
            )
            _record(False, "redirect URL executed")
        except EntityGuardError:
            _record(events == before, "embedded redirect blocked before claim")

    payload = {
        "events": events,
        "seen": seen,
        "notes": notes,
        "entries": [entry.to_dict() for entry in storage._entries.values()]
        if hasattr(storage, "_entries")
        else [],
    }
    dump.write_text(json.dumps(payload, default=str), encoding="utf-8")
    leaked = _scan_payload(payload) or _PAYLOAD in dump.read_text(encoding="utf-8")
    _record(not leaked, "evidence omitted sensitive payload")

    reset_entity_guard_state()
    status = VerificationStatus.FAIL if failed else VerificationStatus.PASS
    return VerificationEvidence(
        scenario="entity-guard",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(notes),
        body_executions=events.count("body"),
        ledger_decisions=notes,
        duration=time.time() - started,
        expected_behavior=(
            "Unauthorized, malformed, dynamic, or undeclared destinations "
            "fail closed before claim. Evidence omits the sensitive payload."
        ),
        observed_behavior="; ".join(notes),
        artifacts=[str(dump), *iso.artifact_paths()],
        limitations=[
            "Synthetic destinations only; no real provider was contacted.",
            "Allowlists are host-controlled; the model cannot add recipients.",
        ],
        status=status,
        summary=(
            "Destination policy blocked unauthorized writes before claim"
            if not failed
            else "Destination policy failed to block an unauthorized write"
        ),
        remediation=""
        if status is VerificationStatus.PASS
        else "Declare host-owned destinations; unknown destination means no execution.",
    )
