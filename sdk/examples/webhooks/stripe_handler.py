"""Stripe webhook dedupe with Mycelium — keyed on the Stripe ``event.id``.

Runnable demo with fakes; no Stripe credentials or HTTP server required.
Requires Python 3.10+ and the SDK importable (run with the SDK venv, or
``pip install -e ./sdk`` then ``python sdk/examples/webhooks/stripe_handler.py``).

Flow: verify signature -> claim on ``event.id`` ->
  COMPLETED / SKIP  -> return 200, no side effect
  PROCEED (IN_FLIGHT) -> do the work once -> complete
  HARD_BLOCK        -> fail closed / operator path

Stripe delivers events at-least-once and retries on non-2xx. The durable
claim turns that into at-most-once handler side effects for each event id:
a redelivery resolves the stored transition instead of re-running the work.
"""

import sys
import tempfile
from pathlib import Path

# Make the SDK importable when run from a checkout (no install needed).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mycelium import (  # noqa: E402
    ActionLedger,
    FileLedgerStorage,
    LedgerHardBlockError,
    TerminalOutcome,
)
from mycelium.transition import (  # noqa: E402
    SideEffectClass,
    ToolTransitionBinding,
)

LEDGER_FILE = Path(tempfile.gettempdir()) / "mycelium-demo-webhooks-stripe.json"

ledger = ActionLedger(storage=FileLedgerStorage(LEDGER_FILE))
binding = ToolTransitionBinding.for_tool(
    agent_id="webhook-worker",
    policy_version="2026.08.1",
    side_effect_class=SideEffectClass.KEYED_MUTATE,
)

TOOL = "handle_stripe_event"

WORK_RUNS: list[str] = []  # demo bookkeeping: which event ids did the work run for?


def verify_signature(payload: bytes, signature: str, secret: str) -> None:
    """Verify the signature BEFORE claiming. Stub: compare to a known secret.

    Real Stripe: construct ``t=<ts>,v1=<mac>`` from the ``Stripe-Signature``
    header and HMAC-SHA256 over ``f"{ts}.{payload}"`` with the webhook signing
    secret (or use ``stripe.Webhook.construct_event``).
    """
    if signature != secret:
        raise PermissionError("signature mismatch")


def fulfill_order(event: dict) -> dict:
    """The side effect. Fake for the example — replace with real order logic."""
    WORK_RUNS.append(event["id"])
    return {"fulfilled": event["data"]["object"]["id"]}


def handle_stripe_event(event: dict) -> int:
    event_id = event["id"]  # key on the Stripe event id (evt_...), NOT the
    args = (event_id,)      # payload bytes, NOT the provider response id, and
    kwargs: dict = {}       # NOT the Idempotency-Key request header.

    request_id = ledger.derive_request_id(
        TOOL, args, kwargs, transition_binding=binding
    )

    try:
        entry = ledger.claim_side_effecting(
            request_id, TOOL, args, kwargs, binding
        )
    except LedgerHardBlockError:
        return 409  # HARD_BLOCK: ambiguous (e.g. crash after a prior attempt);
                    # reconcile or operator-release; never re-run blindly.

    if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
        return 200  # SKIP: this event id was already handled; no side effect.

    ledger.record_decision(
        request_id,
        {"allowed": True, "verdicts": [], "denied_reasons": []},
        expected_owner=entry.owner,
        expected_fence=entry.fence,
    )
    try:
        result = fulfill_order(event)
    except Exception as exc:
        ledger.fail(
            request_id,
            exc,
            failed_after_effect=False,
            expected_fence=entry.fence,
        )
        return 500

    ledger.complete(request_id, result, expected_fence=entry.fence)
    return 200  # PROCEED: the work ran exactly once for this event id.


def _demo() -> None:
    if LEDGER_FILE.exists():
        LEDGER_FILE.unlink()  # deterministic demo: start with a clean ledger

    payload = b'{"id":"evt_1"}'
    verify_signature(payload, "whsec_test", "whsec_test")  # before the claim

    evt_1 = {
        "id": "evt_1",
        "type": "checkout.session.completed",
        "data": {"object": {"id": "cs_1001"}},
    }
    evt_2 = {
        "id": "evt_2",
        "type": "checkout.session.completed",
        "data": {"object": {"id": "cs_1002"}},
    }

    print("delivery 1 (evt_1):", handle_stripe_event(evt_1), "— PROCEED")
    print("delivery 2 (evt_1):", handle_stripe_event(evt_1), "— SKIP")
    print("delivery 3 (evt_2):", handle_stripe_event(evt_2), "— PROCEED")
    print("work ran for:", WORK_RUNS)
    assert WORK_RUNS == ["evt_1", "evt_2"]


if __name__ == "__main__":
    _demo()
