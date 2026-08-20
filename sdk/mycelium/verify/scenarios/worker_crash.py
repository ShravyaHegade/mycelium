"""Worker crash: hard-exit at claim/body/boundary/effect; fail closed."""

from __future__ import annotations

import os
import time
from pathlib import Path

from mycelium.action_ledger import (
    LedgerHardBlockError,
    LedgerWorkerAliveError,
)
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import (
    SYNTHETIC_TOOL,
    SyntheticReconciler,
    count_executions,
    crash_worker,
    join_workers,
    make_ledger,
    make_tool,
    spawn_workers,
    synthetic_binding,
    terminate_owned,
)


def _wait_ready(path: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path) and Path(path).read_text(encoding="utf-8").strip():
            return True
        time.sleep(0.02)
    return False


@verify_scenario("worker-crash")
def run_worker_crash(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    if not iso.multiprocess_capable:
        return VerificationEvidence(
            scenario="worker-crash",
            backend=iso.backend,
            namespace=iso.namespace.prefix,
            duration=time.time() - started,
            expected_behavior="hard worker exit at each boundary is fail-closed",
            observed_behavior="backend cannot share crash state across processes",
            limitations=["memory cannot prove worker-crash durability"],
            status=VerificationStatus.SKIP,
            summary="Worker crash skipped (not multiprocess-capable)",
            remediation="Use file/sqlite/postgres/redis.",
        )

    work = iso.artifact_dir("worker-crash-")
    lease_ttl = 1.0
    decisions: list[str] = []
    failures: list[str] = []
    total_exec = 0
    spec = {
        **iso.worker_payload,
        "run_id": iso.namespace.run_id,
        "prefix_ns": iso.namespace.prefix,
        "lease_ttl": lease_ttl,
        "reclaim_requires_death_signal": True,
    }

    phases = ("after_claim", "after_body_start", "after_boundary", "after_effect")
    try:
        for phase in phases:
            request_id = iso.track(iso.namespace.request_id("crash", phase))
            exec_file = str(work / f"{phase}-exec.txt")
            ready_file = str(work / f"{phase}-ready.txt")
            err_file = str(work / f"{phase}-err.txt")
            effect_file = str(work / f"{phase}-fx.txt")
            payload = {
                **spec,
                "phase": phase,
                "request_id": request_id,
                "exec_file": exec_file,
                "ready_file": ready_file,
                "err_file": err_file,
                "effect_file": effect_file,
                "op_id": f"op-{phase}",
            }
            procs = spawn_workers(crash_worker, [payload])
            ctx.owned_procs.extend(procs)
            if not _wait_ready(ready_file, min(ctx.timeout_seconds, 8.0)):
                failures.append(f"{phase}: worker never reached crash marker")
                terminate_owned(procs)
                continue
            # Hard kill — do not rely on an ordinary exception path.
            for proc in procs:
                if proc.is_alive():
                    proc.kill()
            join_workers(procs, timeout=2.0)
            time.sleep(lease_ttl + 0.3)

            storage = iso.open_fresh_client()
            with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
                blocked = False
                try:
                    tool = make_tool(
                        storage,
                        exec_file,
                        lease_ttl=lease_ttl,
                        poll_timeout=0.4,
                        reclaim_requires_death_signal=True,
                    )
                    tool(1, request_id=request_id)
                except (LedgerHardBlockError, Exception) as exc:
                    blocked = isinstance(exc, LedgerHardBlockError) or "hard" in str(
                        exc
                    ).lower() or "block" in str(exc).lower()

            executions = count_executions(exec_file)
            total_exec += executions
            if phase == "after_claim":
                # Body never started. Blind redispatch must not execute.
                if executions != 0:
                    failures.append(
                        f"{phase}: blind re-execution after crash "
                        f"(executions={executions})"
                    )
                elif not blocked:
                    failures.append(f"{phase}: redispatch was not fail-closed")
                else:
                    decisions.append(f"{phase}:HARD_BLOCK no body")
            elif phase in {"after_body_start", "after_boundary", "after_effect"}:
                if executions != 1:
                    failures.append(
                        f"{phase}: expected 1 crash-window body, got {executions}"
                    )
                elif not blocked:
                    failures.append(f"{phase}: ambiguous crash did not hard-block")
                elif phase == "after_effect":
                    from mycelium.reconcile import ReconcileStatus

                    recon = SyntheticReconciler(
                        status=ReconcileStatus.COMPLETED,
                        result={"charged": True, "reconciled": True},
                    )
                    before = count_executions(exec_file)
                    with execution_scope(
                        TransitionScope(thread_id="verify", run_id="verify")
                    ):
                        tool3 = make_tool(
                            iso.open_fresh_client(),
                            exec_file,
                            reconciler=recon,
                            lease_ttl=lease_ttl,
                            reclaim_requires_death_signal=True,
                        )
                        try:
                            tool3(1, request_id=request_id)
                        except LedgerHardBlockError:
                            failures.append(
                                f"{phase}: COMPLETED reconcile still hard-blocked"
                            )
                        else:
                            after = count_executions(exec_file)
                            if after != before:
                                failures.append(
                                    f"{phase}: reconcile re-executed body ({after - before})"
                                )
                            else:
                                decisions.append(f"{phase}:COMPLETED no re-exec")
                else:
                    decisions.append(f"{phase}:HARD_BLOCK")
            if phase == "after_effect" and not os.path.exists(effect_file):
                failures.append("after_effect: synthetic effect was not recorded")

        # Death-signal: recent heartbeat must not be declared dead.
        death_id = iso.track(iso.namespace.request_id("crash", "death"))
        storage = iso.open_storage()
        binding = synthetic_binding()
        ledger = make_ledger(
            storage,
            binding=binding,
            lease_ttl=30.0,
            reclaim_requires_death_signal=True,
            presumed_dead_after=3600.0,
        )
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            entry = ledger.claim_side_effecting(
                death_id,
                SYNTHETIC_TOOL,
                (1,),
                {
                    "request_id": death_id,
                    "thread_id": "verify",
                    "run_id": "verify",
                },
                binding,
            )
            owner = entry.owner
            recent_refused = False
            try:
                ledger.mark_worker_dead(
                    owner, by="verify-operator", reason="probe live worker"
                )
            except LedgerWorkerAliveError:
                recent_refused = True
            if not recent_refused:
                failures.append("recent heartbeat did not prevent mark_worker_dead")
            stamped = ledger.mark_worker_dead(
                owner,
                by="verify-operator",
                reason="operator kill evidence",
                override_heartbeat=True,
            )
            if not stamped:
                failures.append("operator override produced no death stamp")
            elif "heartbeat overridden" not in (stamped[0].resolution_reason or ""):
                failures.append("operator override was not visibly marked")
            else:
                decisions.append("death-signal: live refuse; override operator-asserted")
            ledger.fail(
                death_id,
                RuntimeError("verify cleanup"),
                expected_fence=entry.fence,
            )
    finally:
        terminate_owned(ctx.owned_procs)

    ok = not failures
    limitations: list[str] = []
    if iso.backend in {"file", "sqlite"}:
        limitations.append("single-node verification only")
    return VerificationEvidence(
        scenario="worker-crash",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(phases),
        body_executions=total_exec,
        ledger_decisions=decisions,
        terminal_outcome="HARD_BLOCK",
        duration=time.time() - started,
        expected_behavior=(
            "no blind re-execution after ambiguous crash; known-not-executed "
            "may reclaim via NOT_EXECUTED; death-signal required; live workers "
            "are not reclaimed"
        ),
        observed_behavior="; ".join(failures or decisions),
        artifacts=[str(work)],
        limitations=limitations,
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary=(
            "ambiguous boundary hard-blocked" if ok else "; ".join(failures)[:200]
        ),
        remediation="" if ok else "Inspect crash-window gates and death-signal policy.",
    )
