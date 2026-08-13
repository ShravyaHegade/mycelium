"""Ambiguous effect: never blindly retry when a side effect may have occurred."""

from __future__ import annotations

import time

from mycelium.action_ledger import LedgerHardBlockError
from mycelium.reconcile import ReconcileStatus
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import (
    SyntheticProvider,
    SyntheticReconciler,
    count_executions,
    make_tool,
)


@verify_scenario("ambiguous-effect")
def run_ambiguous_effect(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    artifact = iso.artifact_file("ambiguous-")
    provider = SyntheticProvider()
    decisions: list[str] = []
    failures: list[str] = []

    # Fail after synthetic provider records the effect, then redispatch.
    rid = iso.track(iso.namespace.request_id("ambiguous", "with-ref"))
    storage = iso.open_storage()
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            storage,
            artifact,
            provider=provider,
            fail_after_effect=True,
            record_ref=True,
        )
        try:
            tool(1, request_id=rid, op_id="with-ref")
        except RuntimeError:
            pass
    if provider.effects.count("with-ref") != 1:
        failures.append("synthetic provider did not record the first effect")

    # COMPLETED: return recorded result, no re-exec.
    completed = SyntheticReconciler(
        status=ReconcileStatus.COMPLETED, result={"charged": True, "op_id": "with-ref"}
    )
    before = count_executions(artifact)
    effects_before = len(provider.effects)
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_fresh_client(),
            artifact,
            provider=provider,
            reconciler=completed,
        )
        result = tool(1, request_id=rid, op_id="with-ref")
    if count_executions(artifact) != before or len(provider.effects) != effects_before:
        failures.append("COMPLETED re-executed the synthetic provider")
    elif result != {"charged": True, "op_id": "with-ref"}:
        failures.append(f"COMPLETED returned {result!r}")
    else:
        decisions.append("COMPLETED:stored result, no re-exec")

    # NOT_EXECUTED: exactly one controlled re-execution.
    rid2 = iso.track(iso.namespace.request_id("ambiguous", "not-exec"))
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_storage(),
            artifact,
            provider=provider,
            fail_after_effect=True,
        )
        try:
            tool(1, request_id=rid2, op_id="not-exec")
        except RuntimeError:
            pass
    not_exec = SyntheticReconciler(status=ReconcileStatus.NOT_EXECUTED)
    before = count_executions(artifact)
    effects_before = len(provider.effects)
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_fresh_client(),
            artifact,
            provider=provider,
            reconciler=not_exec,
            fail_after_effect=False,
        )
        tool(1, request_id=rid2, op_id="not-exec")
    if count_executions(artifact) != before + 1:
        failures.append("NOT_EXECUTED did not authorize exactly one re-execution")
    elif len(provider.effects) != effects_before + 1:
        failures.append("NOT_EXECUTED did not produce exactly one provider effect")
    else:
        decisions.append("NOT_EXECUTED:exactly one re-exec")

    # UNKNOWN / delayed / conflicting / missing evidence hard-block.
    for label, recon in (
        ("unknown", SyntheticReconciler(status=ReconcileStatus.UNKNOWN)),
        ("delayed", SyntheticReconciler(delay_matches=True)),
        ("conflict", SyntheticReconciler(conflicting=True)),
        ("timeout", SyntheticReconciler(raise_error=True)),
    ):
        rid_u = iso.track(iso.namespace.request_id("ambiguous", label))
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            tool = make_tool(
                iso.open_storage(),
                artifact,
                provider=provider,
                fail_after_effect=True,
            )
            try:
                tool(1, request_id=rid_u, op_id=label)
            except RuntimeError:
                pass
        before = count_executions(artifact)
        effects_before = len(provider.effects)
        blocked = False
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            tool = make_tool(
                iso.open_fresh_client(),
                artifact,
                provider=provider,
                reconciler=recon,
            )
            try:
                tool(1, request_id=rid_u, op_id=label)
            except LedgerHardBlockError:
                blocked = True
            except Exception as exc:  # noqa: BLE001
                blocked = "block" in str(exc).lower() or "unknown" in str(exc).lower()
        extra = count_executions(artifact) != before
        if not blocked or extra or len(provider.effects) != effects_before:
            failures.append(f"{label}: ambiguous state became ALLOW")
        else:
            decisions.append(f"{label}:HARD_BLOCK")

    # Missing external_operation_ref must not silently ALLOW.
    rid_m = iso.track(iso.namespace.request_id("ambiguous", "no-ref"))
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_storage(),
            artifact,
            provider=provider,
            fail_after_effect=True,
            record_ref=False,
        )
        try:
            tool(1, request_id=rid_m, op_id="no-ref")
        except RuntimeError:
            pass
    before = count_executions(artifact)
    blocked = False
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            iso.open_fresh_client(),
            artifact,
            provider=provider,
            reconciler=SyntheticReconciler(status=ReconcileStatus.NOT_EXECUTED),
        )
        try:
            tool(1, request_id=rid_m, op_id="no-ref")
        except LedgerHardBlockError:
            blocked = True
        except Exception as exc:  # noqa: BLE001
            blocked = "block" in str(exc).lower()
    if not blocked or count_executions(artifact) != before:
        failures.append("missing external_operation_ref did not hard-block")
    else:
        decisions.append("missing-ref:HARD_BLOCK")

    ok = not failures
    return VerificationEvidence(
        scenario="ambiguous-effect",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=len(decisions) + len(failures),
        body_executions=count_executions(artifact),
        ledger_decisions=decisions,
        terminal_outcome="HARD_BLOCK",
        duration=time.time() - started,
        expected_behavior=(
            "COMPLETED returns stored result; NOT_EXECUTED allows one re-exec; "
            "UNKNOWN/delayed/conflict/timeout/missing-ref hard-block; no "
            "unauthorized duplicate provider effects"
        ),
        observed_behavior="; ".join(failures or decisions),
        artifacts=[artifact],
        limitations=["synthetic provider only; does not prove a real business provider"],
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary="no unauthorized duplicate" if ok else "; ".join(failures)[:200],
        remediation="" if ok else "Ambiguous effects must hard-block until reconciled.",
    )
