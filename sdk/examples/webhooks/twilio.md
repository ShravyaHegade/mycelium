# Twilio webhook dedupe

Key the Mycelium transition on the Twilio **SID** — the `SM...` / `MM...`
message SID (or an `EV...` status-event SID) that identifies the inbound
message.

Twilio delivers at-least-once and retries on non-2xx. Claim the SID through an
`ActionLedger` and you get **at-most-once handler side effects for that SID**:
the first delivery does the work and `complete`s; a retry hits the
`RETURN`/SKIP path and returns `200` without re-running.

Key on the SID — not the message body, not a provider *response* id you mint
later. Because only the SID is in the args fingerprint, a retry with a
slightly different body still resolves to the same transition. Pin `agent_id`
and `policy_version` across deploys so the key stays stable after a release.

Verify the `X-Twilio-Signature` header (HMAC-SHA1 over `url + params` with
your `AUTH_TOKEN`) before claiming. On `HARD_BLOCK` (ambiguous), fail closed —
reconcile or use the operator-release path; never re-run blindly.

```python
from mycelium import (
    ActionLedger, FileLedgerStorage, LedgerHardBlockError, TerminalOutcome,
)
from mycelium.transition import SideEffectClass, ToolTransitionBinding

ledger = ActionLedger(storage=FileLedgerStorage("./twilio-messages.json"))
binding = ToolTransitionBinding.for_tool(
    agent_id="webhook-worker", policy_version="2026.08.1",
    side_effect_class=SideEffectClass.KEYED_MUTATE,
)

def handle_twilio_message(msg: dict) -> int:
    sid = msg["sid"]                  # key on the SID (SM... / MM... / EV...)
    args, kwargs = (sid,), {}         # not the body, not a response id
    request_id = ledger.derive_request_id(
        "handle_twilio_message", args, kwargs, transition_binding=binding,
    )
    try:
        entry = ledger.claim_side_effecting(
            request_id, "handle_twilio_message", args, kwargs, binding)
    except LedgerHardBlockError:
        return 409                     # HARD_BLOCK: reconcile / operator release
    if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
        return 200                     # SKIP: already handled this SID
    ledger.record_decision(
        request_id, {"allowed": True, "verdicts": [], "denied_reasons": []},
        expected_owner=entry.owner, expected_fence=entry.fence,
    )
    try:
        result = process_incoming_sms(msg)  # the side effect, once
    except Exception as exc:
        ledger.fail(request_id, exc, failed_after_effect=False,
                    expected_fence=entry.fence)
        return 500
    ledger.complete(request_id, result, expected_fence=entry.fence)
    return 200                         # PROCEED
```

Runnable demo (fakes only, no credentials):

```bash
python sdk/examples/webhooks/twilio_handler.py
# message 1: 200 — PROCEED   (work ran)
# retry:     200 — SKIP      (same SID, no second run)
```
