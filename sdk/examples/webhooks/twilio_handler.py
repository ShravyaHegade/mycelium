"""Twilio webhook dedupe with Mycelium — keyed on the message / event SID.

Runnable demo with fakes; no Twilio credentials or HTTP server required.
Requires Python 3.10+ and the SDK importable.

Flow: verify signature -> claim on the SID ->
  COMPLETED / SKIP  -> return 200, no side effect
  PROCEED (IN_FLIGHT) -> do the work once -> complete
  HARD_BLOCK        -> fail closed / operator path

Twilio delivers inbound messages/events at-least-once and retries on non-2xx.
The durable claim turns that into at-most-once handler side effects for each
SID (a ``SM...`` / ``MM...`` message SID, or an ``EV...`` status event SID).
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

LEDGER_FILE = Path(tempfile.gettempdir()) / "mycelium-demo-webhooks-twilio.json"

ledger = ActionLedger(storage=FileLedgerStorage(LEDGER_FILE))
binding = ToolTransitionBinding.for_tool(
    agent_id="webhook-worker",
    policy_version="2026.08.1",
    side_effect_class=SideEffectClass.KEYED_MUTATE,
)

TOOL = "handle_twilio_message"

WORK_RUNS: list[str] = []  # demo bookkeeping: which SIDs did the work run for?


def verify_signature(url: str, params: dict, signature: str, auth_token: str) -> None:
    """Verify the signature BEFORE claiming. Stub: compare to a known token.

    Real Twilio: HMAC-SHA1 over ``url + sorted form-encoded params`` with your
    ``AUTH_TOKEN``, compared to the ``X-Twilio-Signature`` header.
    """
    if signature != auth_token:
        raise PermissionError("signature mismatch")


def process_incoming_sms(msg: dict) -> dict:
    """The side effect. Fake for the example — replace with real handling."""
    WORK_RUNS.append(msg["sid"])
    return {"acknowledged": msg["sid"]}


def handle_twilio_message(msg: dict) -> int:
    sid = msg["sid"]        # key on the Twilio message SID (SM... / MM...),
    args = (sid,)           # not the payload body and not a provider
    kwargs: dict = {}       # response id you mint later.

    request_id = ledger.derive_request_id(
        TOOL, args, kwargs, transition_binding=binding
    )

    try:
        entry = ledger.claim_side_effecting(
            request_id, TOOL, args, kwargs, binding
        )
    except LedgerHardBlockError:
        return 409  # HARD_BLOCK: ambiguous; reconcile or operator-release.

    if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
        return 200  # SKIP: this SID was already handled.

    try:
        result = process_incoming_sms(msg)
    except Exception as exc:
        ledger.fail(request_id, exc, failed_after_effect=False)
        return 500

    ledger.complete(request_id, result)
    return 200  # PROCEED: the work ran exactly once for this SID.


def _demo() -> None:
    if LEDGER_FILE.exists():
        LEDGER_FILE.unlink()  # deterministic demo: start with a clean ledger

    url = "https://example.com/webhooks/twilio"
    params = {"From": "+15551234567", "Body": "hello"}
    verify_signature(url, params, "auth_token_test", "auth_token_test")

    msg_1 = {"sid": "SM1001", "From": "+15551234567", "Body": "hello"}
    msg_2 = {"sid": "SM1002", "From": "+15557654321", "Body": "hi"}

    print("message 1 (SM1001):", handle_twilio_message(msg_1), "— PROCEED")
    print("retry    (SM1001):", handle_twilio_message(msg_1), "— SKIP")
    print("message 2 (SM1002):", handle_twilio_message(msg_2), "— PROCEED")
    print("work ran for:", WORK_RUNS)
    assert WORK_RUNS == ["SM1001", "SM1002"]


if __name__ == "__main__":
    _demo()
