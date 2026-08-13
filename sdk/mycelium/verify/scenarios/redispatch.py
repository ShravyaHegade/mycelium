"""Redispatch: same business identity must not re-execute the synthetic body."""

from __future__ import annotations

import time

from mycelium.action_ledger import LedgerHardBlockError
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import count_executions, make_tool


@verify_scenario("redispatch")
def run_redispatch(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    request_id = iso.track(iso.namespace.request_id("redispatch", "stable"))
    artifact = iso.artifact_file("redispatch-")
    storage = iso.open_storage()
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(storage, artifact)
        first = tool(1, request_id=request_id)
        second = tool(1, request_id=request_id)

    restarted = iso.open_fresh_client()
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool2 = make_tool(restarted, artifact)
        third = tool2(1, request_id=request_id)
        drift_blocked = False
        try:
            tool2(2, request_id=request_id)
        except LedgerHardBlockError:
            drift_blocked = True
        except Exception as exc:  # noqa: BLE001
            drift_blocked = "drift" in str(exc).lower() or "differ" in str(exc).lower()

    executions = count_executions(artifact)
    limitations: list[str] = []
    observed = (
        f"body_executions={executions}; first={first!r}; "
        f"second={second!r}; third={third!r}; drift_blocked={drift_blocked}"
    )
    same_result = first == second == third
    no_duplicate = executions == 1
    if not iso.restart_capable:
        limitations.append(
            "memory backend cannot prove restart durability; "
            "reconstructed client shares process-local state"
        )
        # Do not PASS restart-unproven backends even if in-process redispatch is clean.
        if no_duplicate and same_result and drift_blocked:
            status = VerificationStatus.WARN
            summary = "in-process redispatch observed; restart not proven"
        else:
            status = VerificationStatus.FAIL
            summary = f"unauthorized duplicate or drift not blocked ({observed})"
    elif no_duplicate and same_result and drift_blocked:
        status = VerificationStatus.PASS
        summary = f"body executions: {executions} / dispatches: 3"
    else:
        status = VerificationStatus.FAIL
        summary = f"unauthorized duplicate or drift not blocked ({observed})"
    return VerificationEvidence(
        scenario="redispatch",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=3,
        body_executions=executions,
        ledger_decisions=["ALLOW", "RETURN", "RETURN"],
        terminal_outcome="COMPLETED",
        duration=time.time() - started,
        expected_behavior=(
            "body_executions==1; redispatches return stored result; "
            "identity drift fail-closed"
        ),
        observed_behavior=observed,
        artifacts=[artifact],
        limitations=limitations,
        status=status,
        summary=summary,
        remediation=(
            ""
            if status == VerificationStatus.PASS
            else "Use a durable ActionLedger and host-owned request_id; "
            "on_args_drift must fail closed for the same ticket."
        ),
    )
