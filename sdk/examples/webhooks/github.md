# GitHub webhook dedupe

Key the Mycelium transition on the **`X-GitHub-Delivery`** header value (a
per-delivery GUID, e.g. `d1a...`).

GitHub delivers at-least-once; failed attempts retry with the **same**
`X-GitHub-Delivery` GUID, so keying on it dedupes retries. Claim the delivery
id through an `ActionLedger` and you get **at-most-once handler side effects
for that delivery id**: the first attempt does the work and `complete`s; the
retry hits the `RETURN`/SKIP path and returns `200` without re-running.

Two nuances:
- A manual **Redeliver** (UI or API) mints a *new* `X-GitHub-Delivery` — that
  is a genuinely new webhook delivery, so a new transition is correct.
- Deploy-style work is not naturally idempotent: use
  `NON_IDEMPOTENT_MUTATE` so an ambiguous retry **hard-blocks** (fail closed)
  instead of re-running a build. Key on the delivery id — not the payload,
  not the GitHub `X-GitHub-Event` type or installation id.

Verify the `X-Hub-Signature-256` header (HMAC-SHA256 with your webhook secret)
before claiming. On `HARD_BLOCK`, reconcile or use the operator-release path;
never re-run blindly.

```python
from mycelium import (
    ActionLedger, FileLedgerStorage, LedgerHardBlockError, TerminalOutcome,
)
from mycelium.transition import SideEffectClass, ToolTransitionBinding

ledger = ActionLedger(storage=FileLedgerStorage("./github-deliveries.json"))
binding = ToolTransitionBinding.for_tool(
    agent_id="webhook-worker", policy_version="2026.08.1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)

def handle_github_delivery(delivery_id: str, event: dict) -> int:
    args, kwargs = (delivery_id,), {}  # key on X-GitHub-Delivery
    request_id = ledger.derive_request_id(
        "handle_github_delivery", args, kwargs, transition_binding=binding,
    )
    try:
        entry = ledger.claim_side_effecting(
            request_id, "handle_github_delivery", args, kwargs, binding)
    except LedgerHardBlockError:
        return 409                      # HARD_BLOCK: reconcile / operator release
    if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
        return 200                      # SKIP: already handled this delivery id
    try:
        result = run_build(event)       # the side effect, once
    except Exception as exc:
        ledger.fail(request_id, exc, failed_after_effect=False)
        return 500
    ledger.complete(request_id, result)
    return 200                          # PROCEED
```

Runnable demo (fakes only, no credentials):

```bash
python sdk/examples/webhooks/github_handler.py
# delivery 1: 200 — PROCEED   (work ran)
# retry:       200 — SKIP      (same X-GitHub-Delivery, no second run)
```
