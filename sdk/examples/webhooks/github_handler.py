"""GitHub webhook dedupe with Mycelium — keyed on ``X-GitHub-Delivery``.

Runnable demo with fakes; no GitHub credentials or HTTP server required.
Requires Python 3.10+ and the SDK importable.

Flow: verify signature -> claim on the delivery id ->
  COMPLETED / SKIP  -> return 200, no side effect
  PROCEED (IN_FLIGHT) -> do the work once -> complete
  HARD_BLOCK        -> fail closed / operator path

GitHub delivers at-least-once; failed attempts retry with the SAME
``X-GitHub-Delivery`` GUID, so keying on it dedupes retries. A manual
"Redeliver" from the UI/API mints a NEW delivery id — the same event content
then resolves to a new transition, which is the correct (new) webhook delivery.
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

LEDGER_FILE = Path(tempfile.gettempdir()) / "mycelium-demo-webhooks-github.json"

ledger = ActionLedger(storage=FileLedgerStorage(LEDGER_FILE))
binding = ToolTransitionBinding.for_tool(
    agent_id="webhook-worker",
    policy_version="2026.08.1",
    # Deploy-style work is not naturally idempotent: on an ambiguous retry we
    # fail closed (HARD_BLOCK) instead of re-running the build.
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)

TOOL = "handle_github_delivery"

WORK_RUNS: list[str] = []  # demo bookkeeping: which deliveries did work run for?


def verify_signature(payload: bytes, signature: str, secret: str) -> None:
    """Verify the signature BEFORE claiming. Stub: compare to a known secret.

    Real GitHub: HMAC-SHA256 over ``payload`` with the webhook secret, hex-digest
    prefixed ``sha256=``, compared to the ``X-Hub-Signature-256`` header.
    """
    if signature != secret:
        raise PermissionError("signature mismatch")


def run_build(event: dict) -> dict:
    """The side effect. Fake for the example — replace with real build/deploy."""
    WORK_RUNS.append(event["delivery_id"])
    return {"started": event["repository"]["full_name"]}


def handle_github_delivery(delivery_id: str, event: dict) -> int:
    # Key on the X-GitHub-Delivery header value (a per-delivery GUID), not the
    # payload, and not the GitHub event / installation ids.
    args = (delivery_id,)
    kwargs: dict = {}

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
        return 200  # SKIP: this delivery id was already handled.

    ledger.record_decision(
        request_id,
        {"allowed": True, "verdicts": [], "denied_reasons": []},
        expected_owner=entry.owner,
        expected_fence=entry.fence,
    )
    try:
        result = run_build(event)
    except Exception as exc:
        ledger.fail(
            request_id,
            exc,
            failed_after_effect=False,
            expected_fence=entry.fence,
        )
        return 500

    ledger.complete(request_id, result, expected_fence=entry.fence)
    return 200  # PROCEED: the work ran exactly once for this delivery id.


def _demo() -> None:
    if LEDGER_FILE.exists():
        LEDGER_FILE.unlink()  # deterministic demo: start with a clean ledger

    payload = b'{"ref":"refs/heads/main"}'
    verify_signature(payload, "sha256=ok", "sha256=ok")  # before the claim

    evt = {"delivery_id": "deliv_1", "repository": {"full_name": "acme/app"}}
    retry = {"delivery_id": "deliv_1", "repository": {"full_name": "acme/app"}}

    print("delivery 1 (deliv_1):", handle_github_delivery("deliv_1", evt), "— PROCEED")
    print("retry     (deliv_1):", handle_github_delivery("deliv_1", retry), "— SKIP")
    print("work ran for:", WORK_RUNS)
    assert WORK_RUNS == ["deliv_1"]


if __name__ == "__main__":
    _demo()
