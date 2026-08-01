# Stripe webhook dedupe

Key the Mycelium transition on the Stripe **`event.id`** (e.g. `evt_3...`).

Stripe delivers events **at-least-once** and retries on non-2xx. Claim the
`event.id` through an `ActionLedger` and you get **at-most-once handler side
effects for that event id**: the first delivery does the work and `complete`s;
a redelivery hits the `RETURN`/SKIP path and returns `200` without re-running.

Key on `event.id` — not the payload bytes, not the provider *response* id, and
not the `Idempotency-Key` request header (that header dedupes requests *you*
send to Stripe; it is unrelated to inbound event delivery). Because only the
`event.id` is in the args fingerprint, a redelivery with slightly different
payload bytes still resolves to the same transition. Pin `agent_id` and
`policy_version` across deploys so the key stays stable after a release.

Verify the `Stripe-Signature` header before claiming. On `HARD_BLOCK`
(ambiguous, e.g. crash after a prior attempt), fail closed — reconcile with
the Stripe API or use the operator-release path; never re-run blindly.

```python
from mycelium import (
    ActionLedger, FileLedgerStorage, LedgerHardBlockError, TerminalOutcome,
)
from mycelium.transition import SideEffectClass, ToolTransitionBinding

ledger = ActionLedger(storage=FileLedgerStorage("./stripe-events.json"))
binding = ToolTransitionBinding.for_tool(
    agent_id="webhook-worker", policy_version="2026.08.1",
    side_effect_class=SideEffectClass.KEYED_MUTATE,
)

def handle_stripe_event(event: dict) -> int:
    event_id = event["id"]            # key on event.id (evt_...)
    args, kwargs = (event_id,), {}    # not the payload / response id / Idempotency-Key
    request_id = ledger.derive_request_id(
        "handle_stripe_event", args, kwargs, transition_binding=binding,
    )
    try:
        entry = ledger.claim_side_effecting(
            request_id, "handle_stripe_event", args, kwargs, binding)
    except LedgerHardBlockError:
        return 409                     # HARD_BLOCK: reconcile / operator release
    if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
        return 200                     # SKIP: already handled this event id
    try:
        result = fulfill_order(event)  # the side effect, once
    except Exception as exc:
        ledger.fail(request_id, exc, failed_after_effect=False)
        return 500
    ledger.complete(request_id, result)
    return 200                         # PROCEED
```

Runnable demo (fakes only, no credentials):

```bash
python sdk/examples/webhooks/stripe_handler.py
# delivery 1: 200 — PROCEED   (work ran)
# delivery 2: 200 — SKIP      (redelivery, no second run)
```
