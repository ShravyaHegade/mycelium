"""Deterministic exhaustive interleavings for the EffectState protocol."""

from __future__ import annotations

import threading
import time
from dataclasses import replace
from typing import Any

from mycelium.action_ledger import (
    ActionLedger,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    LedgerOutcomeAlreadySetError,
)
from mycelium.reconcile import ReconcileResult
from mycelium.transition import (
    EffectState,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    derive_effect_id_for_call,
    execution_scope,
)
from mycelium.verify.invariants import (
    check_at_most_one_committed_effect_state,
    check_effect_state_consistency,
    check_unique_effect_id_index,
)
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus

_TOOL = "verify_state_machine"
_SCOPE = TransitionScope(thread_id="verify", run_id="verify")


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="mycelium-verify",
        policy_version="verify",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _decision(allowed: bool) -> dict[str, Any]:
    return {"allowed": allowed, "verdicts": [], "denied_reasons": []}


def _new_ledger(
    storage: InMemoryLedgerStorage,
    *,
    reconciler: Any | None = None,
) -> ActionLedger:
    return ActionLedger(
        storage=storage,
        reconciler=reconciler,
        lease_ttl=0.1,
        lease_renew_interval=0,
        poll_interval=0.001,
        poll_timeout=0.05,
        reclaim_requires_death_signal=False,
    )


def _resume_storage(storage: InMemoryLedgerStorage) -> InMemoryLedgerStorage:
    resumed = InMemoryLedgerStorage()
    for entry in storage.list_all():
        resumed.set(LedgerEntry.from_dict(entry.to_dict()))
    return resumed


def _assert_invariants(
    *,
    label: str,
    storage: InMemoryLedgerStorage,
    failures: list[str],
) -> None:
    entries = storage.list_all()
    for violation in check_at_most_one_committed_effect_state(entries):
        failures.append(f"{label}: {violation.message}")
    for violation in check_effect_state_consistency(entries):
        failures.append(f"{label}: {violation.message}")
    for violation in check_unique_effect_id_index(entries):
        failures.append(f"{label}: {violation.message}")


def _expect_stale_refusal(
    fn: Any,
    *,
    label: str,
    failures: list[str],
) -> None:
    try:
        fn()
    except LedgerOutcomeAlreadySetError:
        return
    failures.append(f"{label}: stale write unexpectedly succeeded")


def _run_stale_fence_interleaving(failures: list[str], decisions: list[str]) -> None:
    storage = InMemoryLedgerStorage()
    # IDEMPOTENT_MUTATE + SAFE_RETRY allows EXPIRED/not_crossed reclaim so this
    # interleaving can focus on fence CAS refusal, not ambiguous-replay policy.
    binding = ToolTransitionBinding.for_tool(
        agent_id="mycelium-verify",
        policy_version="verify",
        side_effect_class=SideEffectClass.IDEMPOTENT_MUTATE,
    )
    request_id = "sm-stale-fence"
    kwargs = {"amount": 1, "tool_call_id": "sm-stale", "request_id": request_id}

    ledger_a = _new_ledger(storage)
    with execution_scope(_SCOPE):
        claim_a = ledger_a.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
    stale_owner = claim_a.owner
    stale_fence = claim_a.fence

    storage = _resume_storage(storage)
    expired = storage.get(request_id)
    if expired is None:
        failures.append("stale-fence: initial claim missing")
        return
    storage.set(replace(expired, lease_until=time.time() - 1.0, last_heartbeat_at=None))

    storage = _resume_storage(storage)
    ledger_b = _new_ledger(storage)
    with execution_scope(_SCOPE):
        claim_b = ledger_b.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
    if claim_b.fence <= stale_fence:
        failures.append(
            f"stale-fence: reclaim fence did not advance ({stale_fence} -> {claim_b.fence})"
        )
        return

    storage = _resume_storage(storage)
    ledger_b = _new_ledger(storage)
    with execution_scope(_SCOPE):
        ledger_b.record_decision(
            request_id,
            _decision(True),
            expected_owner=claim_b.owner,
            expected_fence=claim_b.fence,
        )

    storage = _resume_storage(storage)
    stale = _new_ledger(storage)
    with execution_scope(_SCOPE):
        _expect_stale_refusal(
            lambda: stale.record_decision(
                request_id,
                _decision(True),
                expected_owner=stale_owner,
                expected_fence=stale_fence,
            ),
            label="stale-fence/record_decision",
            failures=failures,
        )
        _expect_stale_refusal(
            lambda: stale.advance_boundary(
                request_id,
                SideEffectBoundary.MAYBE_CROSSED,
                expected_owner=stale_owner,
                expected_fence=stale_fence,
            ),
            label="stale-fence/advance_boundary",
            failures=failures,
        )
        _expect_stale_refusal(
            lambda: stale.fail(
                request_id,
                RuntimeError("stale failure"),
                _expected_owner=stale_owner,
                _expected_fence=stale_fence,
            ),
            label="stale-fence/fail",
            failures=failures,
        )
        _expect_stale_refusal(
            lambda: stale.complete(
                request_id,
                {"stale": True},
                _expected_owner=stale_owner,
                _expected_fence=stale_fence,
            ),
            label="stale-fence/complete",
            failures=failures,
        )

    storage = _resume_storage(storage)
    winner = _new_ledger(storage)
    with execution_scope(_SCOPE):
        winner.complete(
            request_id,
            {"winner": "B"},
            _expected_owner=claim_b.owner,
            _expected_fence=claim_b.fence,
        )
    _assert_invariants(label="stale-fence", storage=storage, failures=failures)
    if not any(item.startswith("stale-fence") for item in failures):
        decisions.append("stale-fence: stale complete/fail/decision/boundary writes were refused")


def _run_transition_matrix_cases(failures: list[str], decisions: list[str]) -> None:
    """Enumerate the legal EffectState transitions from the transition-matrix tests."""
    binding = _binding()
    storage = InMemoryLedgerStorage()

    # INTENDED -> ATTEMPTING -> COMMITTED
    request_id = "sm-matrix-committed"
    kwargs = {"amount": 1, "tool_call_id": "sm-matrix-committed", "request_id": request_id}
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        claim = ledger.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        ledger.record_decision(
            request_id,
            _decision(True),
            expected_owner=claim.owner,
            expected_fence=claim.fence,
        )
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        committed = ledger.complete(
            request_id,
            {"ok": True},
            _expected_owner=claim.owner,
            _expected_fence=claim.fence,
        )
    if committed.resolved_effect_state() != EffectState.COMMITTED:
        failures.append("matrix/committed: expected COMMITTED terminal state")

    # INTENDED -> ABORTED
    request_id = "sm-matrix-aborted-denied"
    kwargs = {"amount": 1, "tool_call_id": "sm-matrix-denied", "request_id": request_id}
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        denied_claim = ledger.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        denied = ledger.record_decision(
            request_id,
            _decision(False),
            expected_owner=denied_claim.owner,
            expected_fence=denied_claim.fence,
        )
    if denied.resolved_effect_state() != EffectState.ABORTED:
        failures.append("matrix/aborted-denied: expected ABORTED state")

    # ATTEMPTING -> UNKNOWN
    request_id = "sm-matrix-unknown"
    kwargs = {"amount": 1, "tool_call_id": "sm-matrix-unknown", "request_id": request_id}
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        unknown_claim = ledger.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        ledger.record_decision(
            request_id,
            _decision(True),
            expected_owner=unknown_claim.owner,
            expected_fence=unknown_claim.fence,
        )
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        unknown = ledger.mark_unknown(
            request_id,
            _expected_owner=unknown_claim.owner,
            expected_fence=unknown_claim.fence,
            error="ambiguous",
        )
    if unknown.resolved_effect_state() != EffectState.UNKNOWN:
        failures.append("matrix/unknown: expected UNKNOWN state")

    # ATTEMPTING -> ABORTED
    request_id = "sm-matrix-aborted-fail"
    kwargs = {"amount": 1, "tool_call_id": "sm-matrix-fail", "request_id": request_id}
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        failed_claim = ledger.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        ledger.record_decision(
            request_id,
            _decision(True),
            expected_owner=failed_claim.owner,
            expected_fence=failed_claim.fence,
        )
    storage = _resume_storage(storage)
    ledger = _new_ledger(storage)
    with execution_scope(_SCOPE):
        failed = ledger.fail(
            request_id,
            RuntimeError("failed before effect"),
            failed_after_effect=False,
            _expected_owner=failed_claim.owner,
            _expected_fence=failed_claim.fence,
        )
    if failed.resolved_effect_state() != EffectState.ABORTED:
        failures.append("matrix/aborted-fail: expected ABORTED state")

    _assert_invariants(label="matrix", storage=storage, failures=failures)
    if not any(item.startswith("matrix") for item in failures):
        decisions.append(
            "matrix: legal transitions INTENDED->ATTEMPTING/ABORTED and "
            "ATTEMPTING->COMMITTED/UNKNOWN/ABORTED hold across crash-resume"
        )


class _StaticReconciler:
    def __init__(self, verdict: ReconcileResult) -> None:
        self._verdict = verdict

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        return self._verdict


def _run_reconcile_interleavings(failures: list[str], decisions: list[str]) -> None:
    binding = _binding()
    cases = [
        ("COMPLETED", ReconcileResult.completed({"reconciled": True})),
        ("NOT_EXECUTED", ReconcileResult.not_executed()),
        ("UNKNOWN", ReconcileResult.unknown()),
    ]
    for label, verdict in cases:
        request_id = f"sm-reconcile-{label.lower()}"
        kwargs = {"amount": 1, "tool_call_id": f"sm-{label.lower()}", "request_id": request_id}
        storage = InMemoryLedgerStorage()
        ledger_a = _new_ledger(storage)
        with execution_scope(_SCOPE):
            claim = ledger_a.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
            ledger_a.record_decision(
                request_id,
                _decision(True),
                expected_owner=claim.owner,
                expected_fence=claim.fence,
            )
            ledger_a.attach_external_operation_ref(
                request_id,
                f"op-{label.lower()}",
                expected_owner=claim.owner,
                expected_fence=claim.fence,
            )

        storage = _resume_storage(storage)
        crashed = storage.get(request_id)
        if crashed is None:
            failures.append(f"reconcile-{label}: pre-crash row missing")
            continue
        storage.set(replace(crashed, lease_until=time.time() - 1.0, last_heartbeat_at=None))

        storage = _resume_storage(storage)
        ledger_b = _new_ledger(storage, reconciler=_StaticReconciler(verdict))
        with execution_scope(_SCOPE):
            if label == "UNKNOWN":
                try:
                    ledger_b.claim_side_effecting(request_id, _TOOL, (), dict(kwargs), binding)
                except LedgerHardBlockError:
                    decisions.append("reconcile-unknown: redispatch hard-blocked")
                else:
                    failures.append("reconcile-unknown: expected hard-block, claim returned")
            else:
                recovered = ledger_b.claim_side_effecting(
                    request_id,
                    _TOOL,
                    (),
                    dict(kwargs),
                    binding,
                )
                if label == "COMPLETED":
                    if recovered.resolved_terminal_outcome() != TerminalOutcome.COMPLETED:
                        failures.append(
                            "reconcile-completed: expected COMPLETED outcome after reconcile"
                        )
                    else:
                        decisions.append(
                            "reconcile-completed: provider verdict returned stored result"
                        )
                else:
                    if recovered.resolved_effect_state() != EffectState.INTENDED:
                        failures.append(
                            "reconcile-not-executed: expected fresh INTENDED claim after reset"
                        )
                    else:
                        decisions.append(
                            "reconcile-not-executed: reconcile reset ATTEMPTING to fresh claim"
                        )
        _assert_invariants(label=f"reconcile-{label.lower()}", storage=storage, failures=failures)


def _run_concurrent_intended_claim(failures: list[str], decisions: list[str]) -> None:
    storage = InMemoryLedgerStorage()
    ledger = _new_ledger(storage)
    binding = _binding()
    request_id = "sm-concurrent-intended"
    kwargs = {"amount": 1, "tool_call_id": "sm-concurrent", "request_id": request_id}
    effect_id = derive_effect_id_for_call(_TOOL, (), kwargs, binding)
    with execution_scope(_SCOPE):
        template = ledger._new_inflight_entry(
            request_id,
            _TOOL,
            (),
            dict(kwargs),
            binding=binding,
            _effect_id=effect_id,
        )

    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def _worker() -> None:
        barrier.wait()
        outcome, _existing = storage.try_claim_inflight(template, lease_ttl=1.0)
        outcomes.append(outcome)

    first = threading.Thread(target=_worker, daemon=True)
    second = threading.Thread(target=_worker, daemon=True)
    first.start()
    second.start()
    first.join(timeout=5.0)
    second.join(timeout=5.0)

    if len(outcomes) != 2:
        failures.append(f"concurrent-claim: expected 2 outcomes, got {len(outcomes)}")
    elif outcomes.count("claimed") != 1 or outcomes.count("in_flight") != 1:
        failures.append(
            "concurrent-claim: "
            f"outcomes={outcomes!r}, expected one claimed + one in_flight"
        )
    else:
        decisions.append("concurrent-claim: exactly one INTENDED claim won")
    _assert_invariants(label="concurrent-claim", storage=storage, failures=failures)


@verify_scenario("state-machine-exhaustive")
def run_state_machine_exhaustive(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    failures: list[str] = []
    decisions: list[str] = []

    _run_transition_matrix_cases(failures, decisions)
    _run_stale_fence_interleaving(failures, decisions)
    _run_reconcile_interleavings(failures, decisions)
    _run_concurrent_intended_claim(failures, decisions)

    status = VerificationStatus.PASS if not failures else VerificationStatus.FAIL
    return VerificationEvidence(
        scenario="state-machine-exhaustive",
        backend=ctx.isolation.backend,
        namespace=ctx.isolation.namespace.prefix,
        attempts=9,
        body_executions=0,
        ledger_decisions=decisions,
        terminal_outcome="COMMITTED" if status == VerificationStatus.PASS else None,
        duration=time.time() - started,
        expected_behavior=(
            "deterministic interleavings preserve EffectState invariants: stale-fence writes are "
            "rejected, reconcile outcomes map ATTEMPTING to COMPLETED/INTENDED/UNKNOWN gate "
            "behavior, and concurrent INTENDED claims produce exactly one winner"
        ),
        observed_behavior="; ".join(failures or decisions),
        status=status,
        summary=(
            "state-machine exhaustive interleavings held"
            if status == VerificationStatus.PASS
            else "; ".join(failures)[:220]
        ),
        remediation=(
            ""
            if status == VerificationStatus.PASS
            else (
                "Inspect stale-fence CAS handling, reconcile transitions, "
                "and effect_id index invariants."
            )
        ),
    )
