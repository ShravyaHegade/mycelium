"""Reconcile: conservative, idempotent, and concurrent-safe."""

from __future__ import annotations

import os
import time
from pathlib import Path

from mycelium.action_ledger import LedgerHardBlockError
from mycelium.reconcile import ReconcileStatus
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import (
    SyntheticProvider,
    SyntheticReconciler,
    concurrent_reconcile_failure,
    count_executions,
    join_workers,
    make_tool,
    reconcile_worker,
    spawn_workers,
    terminate_owned,
)


@verify_scenario("reconcile")
def run_reconcile(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    artifact = iso.artifact_file("reconcile-")
    provider = SyntheticProvider()
    decisions: list[str] = []
    failures: list[str] = []

    def _fail_after(suffix: str, **tool_kwargs) -> str:
        rid = iso.track(iso.namespace.request_id("reconcile", suffix))
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            tool = make_tool(
                iso.open_storage(),
                artifact,
                provider=provider,
                fail_after_effect=True,
                **tool_kwargs,
            )
            try:
                kw = {"op_id": suffix}
                if tool_kwargs.get("keyed"):
                    kw["idempotency_key"] = tool_kwargs.get("idempotency_key", "ikey-stable")
                tool(1, request_id=rid, **kw)
            except RuntimeError:
                pass
        return rid

    # Read-only: mutating reconciler must fail closed.
    rid = _fail_after("readonly")
    mutating = SyntheticReconciler(mutating=True)
    blocked = False
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(iso.open_fresh_client(), artifact, provider=provider, reconciler=mutating)
        try:
            tool(1, request_id=rid, op_id="readonly")
        except LedgerHardBlockError:
            blocked = True
        except Exception as exc:  # noqa: BLE001
            blocked = "block" in str(exc).lower()
    if not blocked:
        failures.append("mutating reconciler did not fail closed")
    else:
        decisions.append("read-only:HARD_BLOCK")

    # Idempotent COMPLETED.
    rid = _fail_after("completed")
    completed = SyntheticReconciler(status=ReconcileStatus.COMPLETED, result={"reconciled": True})
    before = count_executions(artifact)
    results = []
    for _ in range(2):
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            tool = make_tool(
                iso.open_fresh_client(),
                artifact,
                provider=provider,
                reconciler=completed,
            )
            results.append(tool(1, request_id=rid, op_id="completed"))
    if results[0] != results[1] or count_executions(artifact) != before:
        failures.append("COMPLETED reconcile was not idempotent")
    else:
        decisions.append("COMPLETED:idempotent")

    # NOT_EXECUTED authorizes at most one re-execution.
    rid = _fail_after("not-exec")
    not_exec = SyntheticReconciler(status=ReconcileStatus.NOT_EXECUTED)
    before = count_executions(artifact)
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_fresh_client(),
            artifact,
            provider=provider,
            reconciler=not_exec,
            fail_after_effect=False,
        )
        tool(1, request_id=rid, op_id="not-exec")
        tool(1, request_id=rid, op_id="not-exec")
    if count_executions(artifact) != before + 1:
        failures.append("NOT_EXECUTED authorized more than one re-execution")
    else:
        decisions.append("NOT_EXECUTED:at most one re-exec")

    # Zero matches in window / conflicts / timeout stay UNKNOWN.
    for label, recon in (
        ("zero", SyntheticReconciler(zero_matches_in_window=True)),
        ("conflict", SyntheticReconciler(conflicting=True)),
        ("timeout", SyntheticReconciler(raise_error=True)),
    ):
        rid = _fail_after(label)
        before = count_executions(artifact)
        blocked = False
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            tool = make_tool(iso.open_fresh_client(), artifact, provider=provider, reconciler=recon)
            try:
                tool(1, request_id=rid, op_id=label)
            except LedgerHardBlockError:
                blocked = True
            except Exception as exc:  # noqa: BLE001
                blocked = "block" in str(exc).lower()
        if not blocked or count_executions(artifact) != before:
            failures.append(f"{label}: did not hard-block")
        else:
            decisions.append(f"{label}:HARD_BLOCK")

    # Provider-key reuse / drift / absence / expiry — test fail-closed
    # before a successful same-key retry can settle the ticket.
    rid = iso.track(iso.namespace.request_id("reconcile", "keyed"))
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_storage(),
            artifact,
            provider=provider,
            keyed=True,
            key_ttl=3600.0,
            fail_before_effect=True,
        )
        try:
            tool(1, request_id=rid, op_id="keyed", idempotency_key="ikey-stable")
        except RuntimeError:
            pass
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_fresh_client(),
            artifact,
            provider=provider,
            keyed=True,
            key_ttl=3600.0,
        )
        drift_blocked = False
        try:
            tool(1, request_id=rid, op_id="keyed", idempotency_key="ikey-other")
        except LedgerHardBlockError:
            drift_blocked = True
        except Exception as exc:  # noqa: BLE001
            drift_blocked = "block" in str(exc).lower() or "key" in str(exc).lower()
        if not drift_blocked:
            failures.append("provider key drift did not fail closed")
        else:
            decisions.append("keyed:drift HARD_BLOCK")
        missing_blocked = False
        try:
            tool(1, request_id=rid, op_id="keyed")
        except LedgerHardBlockError:
            missing_blocked = True
        except Exception as exc:  # noqa: BLE001
            missing_blocked = "block" in str(exc).lower() or "key" in str(exc).lower()
        if not missing_blocked:
            failures.append("missing provider key did not fail closed")
        else:
            decisions.append("keyed:missing HARD_BLOCK")
        before_keys = list(provider.keys_seen)
        try:
            tool(1, request_id=rid, op_id="keyed", idempotency_key="ikey-stable")
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, LedgerHardBlockError):
                decisions.append("keyed:NOT_EXECUTED path")
            else:
                failures.append(f"keyed reuse: {type(exc).__name__}: {exc}")
        else:
            reused = provider.keys_seen[len(before_keys) :]
            if reused and reused[-1] != "ikey-stable":
                failures.append("provider key was not reused across retry")
            else:
                decisions.append("keyed:same key reused")

    # Expired validity window must not authorize blind retry.
    rid = iso.track(iso.namespace.request_id("reconcile", "expired-key"))
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_storage(),
            artifact,
            provider=provider,
            keyed=True,
            key_ttl=0.01,
            fail_after_effect=True,
        )
        try:
            tool(1, request_id=rid, op_id="expired-key", idempotency_key="ikey-exp")
        except RuntimeError:
            pass
    time.sleep(0.05)
    expired_blocked = False
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_fresh_client(),
            artifact,
            provider=provider,
            keyed=True,
            key_ttl=0.01,
            reconciler=SyntheticReconciler(status=ReconcileStatus.UNKNOWN),
        )
        try:
            tool(1, request_id=rid, op_id="expired-key", idempotency_key="ikey-exp")
        except LedgerHardBlockError:
            expired_blocked = True
        except Exception as exc:  # noqa: BLE001
            expired_blocked = "block" in str(exc).lower()
    if not expired_blocked:
        failures.append("expired provider-key window authorized a retry")
    else:
        decisions.append("keyed:expired HARD_BLOCK")

    # Concurrent reconcilers cannot authorize multiple executions.
    if iso.multiprocess_capable and iso.worker_payload.get("backend") != "memory":
        work = iso.artifact_dir("reconcile-mp-")
        rid = _fail_after("concurrent")
        exec_file = str(work / "exec.txt")
        ready_file = str(work / "ready.txt")
        barrier_file = str(work / "barrier.txt")
        spec = {
            **iso.worker_payload,
            "run_id": iso.namespace.run_id,
            "prefix_ns": iso.namespace.prefix,
            "request_id": rid,
            "exec_file": exec_file,
            "out_file": str(work / "out.txt"),
            "err_file": str(work / "err.txt"),
            "ready_file": ready_file,
            "barrier_file": barrier_file,
            "reconcile_status": "NOT_EXECUTED",
            "op_id": "concurrent",
            "poll_timeout": min(ctx.timeout_seconds, 8.0),
        }
        procs = spawn_workers(reconcile_worker, [spec, dict(spec)])
        ctx.owned_procs.extend(procs)
        deadline = time.time() + min(ctx.timeout_seconds, 8.0)
        while time.time() < deadline:
            if (
                os.path.exists(ready_file)
                and len(Path(ready_file).read_text(encoding="utf-8").splitlines()) >= 2
            ):
                break
            time.sleep(0.02)
        Path(barrier_file).write_text("go\n", encoding="utf-8")
        join_workers(procs, timeout=ctx.timeout_seconds)
        extra = count_executions(exec_file)
        reason = concurrent_reconcile_failure(
            procs,
            executions=extra,
            out_file=str(work / "out.txt"),
            err_file=str(work / "err.txt"),
            workers=2,
        )
        if reason is not None:
            failures.append(reason)
        else:
            decisions.append("concurrent:exactly one execution")
        terminate_owned(procs)
    else:
        decisions.append("concurrent:skipped (not multiprocess-capable)")

    ok = not failures
    return VerificationEvidence(
        scenario="reconcile",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(decisions) + len(failures),
        body_executions=count_executions(artifact),
        ledger_decisions=decisions,
        terminal_outcome="CONSERVATIVE",
        duration=time.time() - started,
        expected_behavior=(
            "read-only, idempotent COMPLETED, at most one NOT_EXECUTED re-exec, "
            "zero/conflict/timeout hard-block, keyed provider-key policy, "
            "concurrent reconcilers cannot double-execute"
        ),
        observed_behavior="; ".join(failures or decisions),
        artifacts=[artifact],
        limitations=["synthetic reconciler; does not prove a real provider"],
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary="conservative and idempotent" if ok else "; ".join(failures)[:200],
        remediation="" if ok else "Keep reconciliation read-only and fail-closed.",
    )
