"""Deep deterministic proof scenario for the effect-commit protocol."""

from __future__ import annotations

import time

from mycelium.verify.proof.crash_sweep import (
    run_crash_point_sweeps,
    run_effect_id_alias_crash_sweeps,
    run_expired_unknown_hard_block_sweeps,
    run_fence_takeover_crash_sweeps,
)
from mycelium.verify.proof.interleavings import run_enumerated_properties
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus


@verify_scenario("effect-protocol-proof")
def run_effect_protocol_proof(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    failures: list[str] = []
    decisions: list[str] = []

    for runner in (
        run_crash_point_sweeps,
        run_effect_id_alias_crash_sweeps,
        run_fence_takeover_crash_sweeps,
        run_expired_unknown_hard_block_sweeps,
        run_enumerated_properties,
    ):
        part_failures, part_decisions = runner()
        failures.extend(part_failures)
        decisions.extend(part_decisions)

    ok = not failures
    return VerificationEvidence(
        scenario="effect-protocol-proof",
        backend=ctx.isolation.backend,
        namespace=ctx.isolation.namespace.prefix,
        attempts=len(decisions),
        body_executions=0,
        ledger_decisions=decisions,
        terminal_outcome="COMMITTED" if ok else None,
        duration=time.time() - started,
        expected_behavior=(
            "for every scripted crash point and legal step prefix, crash/resume + "
            "redispatch preserves the effect-commit invariants (at-most-one COMMITTED "
            "per effect_id, effect_id index uniqueness, EffectState consistency); "
            "effect_id alias dedupe and fence takeover interleavings do the same"
        ),
        observed_behavior="; ".join(failures or decisions[:12]),
        limitations=[
            "in-process crash/resume only (no real process kill at every await)",
            "finite enumerated scripts — not unbounded schedule exploration",
        ],
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary=(
            "effect-protocol proof sweeps held"
            if ok
            else "; ".join(failures)[:220]
        ),
        remediation=(
            ""
            if ok
            else "Inspect crash-resume handling, effect_id alias dedupe, and fence CAS paths."
        ),
    )
