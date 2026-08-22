"""Crash/resume sweeps at every protocol step (FoundationDB-style proof aid)."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Literal

from mycelium.action_ledger import InMemoryLedgerStorage, LedgerEntry
from mycelium.transition import (
    EffectState,
    SideEffectBoundary,
    ToolTransitionBinding,
    derive_effect_id_for_call,
    execution_scope,
)
from mycelium.verify.proof.harness import (
    PROOF_TOOL,
    assert_effect_protocol_invariants,
    idempotent_reclaim_binding,
    new_proof_ledger,
    resume_storage,
    standard_proof_binding,
    standard_proof_scope,
)

StepKind = Literal[
    "claim",
    "decision_allow",
    "decision_deny",
    "complete",
    "fail_before",
    "mark_unknown",
    "attach_ref",
    "advance_maybe",
    "advance_crossed",
]

_SCOPE = standard_proof_scope()


@dataclass
class _RunCtx:
    storage: InMemoryLedgerStorage
    request_id: str
    kwargs: dict[str, Any]
    binding: ToolTransitionBinding
    claim: LedgerEntry | None = None
    owner: str | None = None
    fence: int | None = None


def _decision(allowed: bool) -> dict[str, Any]:
    return {"allowed": allowed, "verdicts": [], "denied_reasons": []}


def _sync_owner_fence(ctx: _RunCtx) -> None:
    row = ctx.storage.get(ctx.request_id)
    if row is not None:
        ctx.owner = row.owner
        ctx.fence = row.fence


def _step_claim(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    with execution_scope(_SCOPE):
        ctx.claim = ledger.claim_side_effecting(
            ctx.request_id,
            PROOF_TOOL,
            (),
            dict(ctx.kwargs),
            ctx.binding,
        )
    _sync_owner_fence(ctx)


def _step_decision_allow(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.record_decision(
            ctx.request_id,
            _decision(True),
            expected_owner=ctx.owner,
            expected_fence=ctx.fence,
        )
    _sync_owner_fence(ctx)


def _step_decision_deny(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.record_decision(
            ctx.request_id,
            _decision(False),
            expected_owner=ctx.owner,
            expected_fence=ctx.fence,
        )
    _sync_owner_fence(ctx)


def _step_complete(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.complete(
            ctx.request_id,
            {"ok": True},
            _expected_owner=ctx.owner,
            _expected_fence=ctx.fence,
        )


def _step_fail_before(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.fail(
            ctx.request_id,
            RuntimeError("failed before effect"),
            failed_after_effect=False,
            _expected_owner=ctx.owner,
            _expected_fence=ctx.fence,
        )


def _step_mark_unknown(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.mark_unknown(
            ctx.request_id,
            _expected_owner=ctx.owner,
            expected_fence=ctx.fence,
            error="ambiguous",
        )


def _step_attach_ref(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.attach_external_operation_ref(
            ctx.request_id,
            f"provider-{ctx.request_id}",
            expected_owner=ctx.owner,
            expected_fence=ctx.fence,
        )


def _step_advance_maybe(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.advance_boundary(
            ctx.request_id,
            SideEffectBoundary.MAYBE_CROSSED,
            expected_owner=ctx.owner,
            expected_fence=ctx.fence,
        )


def _step_advance_crossed(ctx: _RunCtx) -> None:
    ledger = new_proof_ledger(ctx.storage)
    _sync_owner_fence(ctx)
    with execution_scope(_SCOPE):
        ledger.advance_boundary(
            ctx.request_id,
            SideEffectBoundary.CROSSED,
            expected_owner=ctx.owner,
            expected_fence=ctx.fence,
        )


_STEP_RUNNERS: dict[StepKind, Callable[[_RunCtx], None]] = {
    "claim": _step_claim,
    "decision_allow": _step_decision_allow,
    "decision_deny": _step_decision_deny,
    "complete": _step_complete,
    "fail_before": _step_fail_before,
    "mark_unknown": _step_mark_unknown,
    "attach_ref": _step_attach_ref,
    "advance_maybe": _step_advance_maybe,
    "advance_crossed": _step_advance_crossed,
}


@dataclass(frozen=True)
class CrashSweepScript:
    name: str
    request_id: str
    tool_call_id: str
    steps: tuple[StepKind, ...]
    binding: ToolTransitionBinding = field(default_factory=standard_proof_binding)


CRASH_SWEEP_SCRIPTS: tuple[CrashSweepScript, ...] = (
    CrashSweepScript(
        "happy-complete",
        "proof-happy",
        "proof-happy-call",
        ("claim", "decision_allow", "complete"),
    ),
    CrashSweepScript(
        "denied-abort",
        "proof-denied",
        "proof-denied-call",
        ("claim", "decision_deny"),
    ),
    CrashSweepScript(
        "fail-before-effect",
        "proof-fail",
        "proof-fail-call",
        ("claim", "decision_allow", "fail_before"),
    ),
    CrashSweepScript(
        "unknown-ambiguity",
        "proof-unknown",
        "proof-unknown-call",
        ("claim", "decision_allow", "mark_unknown"),
    ),
    CrashSweepScript(
        "boundary-then-complete",
        "proof-boundary",
        "proof-boundary-call",
        (
            "claim",
            "decision_allow",
            "advance_maybe",
            "advance_crossed",
            "complete",
        ),
    ),
    CrashSweepScript(
        "reconcile-seed",
        "proof-reconcile",
        "proof-reconcile-call",
        ("claim", "decision_allow", "attach_ref"),
    ),
)


def _kwargs_for(script: CrashSweepScript) -> dict[str, Any]:
    return {
        "amount": 1,
        "tool_call_id": script.tool_call_id,
        "request_id": script.request_id,
    }


def _run_script_prefix(
    script: CrashSweepScript,
    *,
    through_index: int,
    resume_between_steps: bool,
) -> _RunCtx:
    storage = InMemoryLedgerStorage()
    ctx = _RunCtx(
        storage=storage,
        request_id=script.request_id,
        kwargs=_kwargs_for(script),
        binding=script.binding,
    )
    for index, step in enumerate(script.steps):
        if index > through_index:
            break
        _STEP_RUNNERS[step](ctx)
        if resume_between_steps and index < through_index:
            ctx.storage = resume_storage(ctx.storage)
    return ctx


def _continue_after_crash(ctx: _RunCtx, steps: tuple[StepKind, ...]) -> None:
    """Finish the script after a crash/resume with fresh ledger instances."""
    for step in steps:
        ctx.storage = resume_storage(ctx.storage)
        _STEP_RUNNERS[step](ctx)


def run_crash_point_sweeps() -> tuple[list[str], list[str]]:
    """Crash after every step in every script; resume; finish; check invariants."""
    failures: list[str] = []
    decisions: list[str] = []
    total = 0

    for script in CRASH_SWEEP_SCRIPTS:
        for crash_after in range(len(script.steps)):
            total += 1
            crash_step = script.steps[crash_after]
            label = f"crash/{script.name}/after-{crash_step}"
            ctx = _run_script_prefix(script, through_index=crash_after, resume_between_steps=False)
            ctx.storage = resume_storage(ctx.storage)
            _continue_after_crash(ctx, script.steps[crash_after + 1 :])
            failures.extend(assert_effect_protocol_invariants(ctx.storage, label=label))
            if not any(item.startswith(label) for item in failures):
                decisions.append(f"{label}: crash-resume finish preserved invariants")

            total += 1
            multi_label = f"multi-resume/{script.name}/after-{crash_step}"
            ctx = _run_script_prefix(script, through_index=crash_after, resume_between_steps=False)
            ctx.storage = resume_storage(ctx.storage)
            for step in script.steps[crash_after + 1 :]:
                ctx.storage = resume_storage(ctx.storage)
                _STEP_RUNNERS[step](ctx)
            failures.extend(assert_effect_protocol_invariants(ctx.storage, label=multi_label))

    decisions.append(f"crash-point sweeps executed: {total} interleavings")
    return failures, decisions


def run_effect_id_alias_crash_sweeps() -> tuple[list[str], list[str]]:
    """Crash during cross-request_id alias dedupe; assert one canonical row."""
    failures: list[str] = []
    decisions: list[str] = []
    binding = idempotent_reclaim_binding()
    tool_call_id = "proof-alias-call"
    first_request = "proof-alias-req-a"
    second_request = "proof-alias-req-b"
    kwargs_a = {
        "amount": 1,
        "tool_call_id": tool_call_id,
        "request_id": first_request,
        "thread_id": "proof",
        "run_id": "proof",
    }
    kwargs_b = {
        "amount": 1,
        "tool_call_id": tool_call_id,
        "request_id": second_request,
        "thread_id": "proof",
        "run_id": "proof",
    }

    crash_points: tuple[tuple[str, Callable[[], tuple[InMemoryLedgerStorage, str]]], ...] = (
        (
            "after-first-claim",
            lambda: (
                _alias_after_first_claim(binding, kwargs_a, first_request),
                first_request,
            ),
        ),
        (
            "after-second-claim",
            lambda: (
                _alias_after_second_claim(
                    binding, kwargs_a, kwargs_b, first_request, second_request
                ),
                first_request,
            ),
        ),
        (
            "after-decision",
            lambda: (
                _alias_after_decision(
                    binding, kwargs_a, kwargs_b, first_request, second_request
                ),
                first_request,
            ),
        ),
    )

    for label_suffix, builder in crash_points:
        label = f"alias-crash/{label_suffix}"
        storage, canonical = builder()
        storage = resume_storage(storage)
        ledger = new_proof_ledger(storage)
        with execution_scope(_SCOPE):
            if label_suffix == "after-first-claim":
                _expire_lease(storage, canonical)
                ledger.claim_side_effecting(
                    second_request,
                    PROOF_TOOL,
                    (),
                    dict(kwargs_b),
                    binding,
                )
            row = storage.get(canonical)
            if row is None:
                failures.append(f"{label}: missing canonical row after crash prefix")
                continue
            if row.resolved_effect_state() == EffectState.INTENDED:
                ledger.record_decision(
                    canonical,
                    _decision(True),
                    expected_owner=row.owner,
                    expected_fence=row.fence,
                )
            row = storage.get(canonical)
            assert row is not None
            if row.resolved_effect_state() == EffectState.ATTEMPTING:
                ledger.complete(
                    canonical,
                    {"alias": True},
                    _expected_owner=row.owner,
                    _expected_fence=row.fence,
                )
        rows = [
            entry
            for entry in storage.list_all()
            if entry.request_id in {first_request, second_request}
        ]
        if len(rows) != 1:
            failures.append(f"{label}: expected one canonical row, got {len(rows)}")
        else:
            effect_id = derive_effect_id_for_call(
                PROOF_TOOL,
                (),
                kwargs_a,
                binding,
            )
            with execution_scope(_SCOPE):
                resolved = storage.resolve_request_id(effect_id)
            if resolved != first_request:
                failures.append(
                    f"{label}: effect_id index resolved {resolved!r}, expected {first_request!r}"
                )
        failures.extend(assert_effect_protocol_invariants(storage, label=label))
        if not any(item.startswith(label) for item in failures):
            decisions.append(f"{label}: alias dedupe held across crash-resume")

    return failures, decisions


def _alias_after_first_claim(
    binding: ToolTransitionBinding,
    kwargs_a: dict[str, Any],
    first_request: str,
) -> InMemoryLedgerStorage:
    storage = InMemoryLedgerStorage()
    ledger = new_proof_ledger(storage)
    with execution_scope(_SCOPE):
        ledger.claim_side_effecting(first_request, PROOF_TOOL, (), dict(kwargs_a), binding)
    return storage


def _expire_lease(storage: InMemoryLedgerStorage, request_id: str) -> None:
    row = storage.get(request_id)
    if row is not None:
        storage.set(replace(row, lease_until=time.time() - 1.0, last_heartbeat_at=None))


def _alias_after_second_claim(
    binding: ToolTransitionBinding,
    kwargs_a: dict[str, Any],
    kwargs_b: dict[str, Any],
    first_request: str,
    second_request: str,
) -> InMemoryLedgerStorage:
    storage = _alias_after_first_claim(binding, kwargs_a, first_request)
    storage = resume_storage(storage)
    _expire_lease(storage, first_request)
    ledger = new_proof_ledger(storage)
    with execution_scope(_SCOPE):
        ledger.claim_side_effecting(
            second_request,
            PROOF_TOOL,
            (),
            dict(kwargs_b),
            binding,
        )
    return storage


def _alias_after_decision(
    binding: ToolTransitionBinding,
    kwargs_a: dict[str, Any],
    kwargs_b: dict[str, Any],
    first_request: str,
    second_request: str,
) -> InMemoryLedgerStorage:
    storage = _alias_after_second_claim(
        binding, kwargs_a, kwargs_b, first_request, second_request
    )
    storage = resume_storage(storage)
    row = storage.get(first_request)
    assert row is not None
    ledger = new_proof_ledger(storage)
    with execution_scope(_SCOPE):
        ledger.record_decision(
            first_request,
            _decision(True),
            expected_owner=row.owner,
            expected_fence=row.fence,
        )
    return storage


def run_fence_takeover_crash_sweeps() -> tuple[list[str], list[str]]:
    """Two-worker fence takeover with crash/resume between takeover steps."""
    from mycelium.action_ledger import LedgerOutcomeAlreadySetError
    from mycelium.verify.proof.harness import idempotent_reclaim_binding

    failures: list[str] = []
    decisions: list[str] = []
    binding = idempotent_reclaim_binding()
    request_id = "proof-fence-takeover"
    kwargs = {"amount": 1, "tool_call_id": "proof-fence", "request_id": request_id}

    storage = InMemoryLedgerStorage()
    ledger_a = new_proof_ledger(storage, lease_ttl=0.05)
    with execution_scope(_SCOPE):
        claim_a = ledger_a.claim_side_effecting(
            request_id, PROOF_TOOL, (), dict(kwargs), binding
        )
    stale_owner = claim_a.owner
    stale_fence = claim_a.fence

    storage = resume_storage(storage)
    expired = storage.get(request_id)
    if expired is None:
        failures.append("fence-crash: missing initial claim")
        return failures, decisions
    storage.set(replace(expired, lease_until=time.time() - 1.0, last_heartbeat_at=None))

    # Crash after reclaim.
    work = resume_storage(storage)
    ledger_b = new_proof_ledger(work, lease_ttl=30.0)
    with execution_scope(_SCOPE):
        claim_b = ledger_b.claim_side_effecting(
            request_id, PROOF_TOOL, (), dict(kwargs), binding
        )
    if claim_b.fence <= stale_fence:
        failures.append("fence-crash/after-reclaim: fence did not advance")
    else:
        failures.extend(
            assert_effect_protocol_invariants(work, label="fence-crash/after-reclaim")
        )
        if not any(item.startswith("fence-crash/after-reclaim") for item in failures):
            decisions.append("fence-crash/after-reclaim: invariants held")

    # Crash after stale write refusal.
    work = resume_storage(storage)
    ledger_b = new_proof_ledger(work, lease_ttl=30.0)
    with execution_scope(_SCOPE):
        claim_b = ledger_b.claim_side_effecting(
            request_id, PROOF_TOOL, (), dict(kwargs), binding
        )
    work = resume_storage(work)
    stale = new_proof_ledger(work)
    with execution_scope(_SCOPE):
        try:
            stale.complete(
                request_id,
                {"stale": True},
                _expected_owner=stale_owner,
                _expected_fence=stale_fence,
            )
            failures.append("fence-crash/after-stale-refusal: stale complete succeeded")
        except LedgerOutcomeAlreadySetError:
            pass
    failures.extend(
        assert_effect_protocol_invariants(work, label="fence-crash/after-stale-refusal")
    )
    if not any(item.startswith("fence-crash/after-stale-refusal") for item in failures):
        decisions.append("fence-crash/after-stale-refusal: invariants held")

    # Crash after winner completes.
    work = resume_storage(storage)
    ledger_b = new_proof_ledger(work, lease_ttl=30.0)
    with execution_scope(_SCOPE):
        claim_b = ledger_b.claim_side_effecting(
            request_id, PROOF_TOOL, (), dict(kwargs), binding
        )
    work = resume_storage(work)
    winner = new_proof_ledger(work)
    with execution_scope(_SCOPE):
        winner.record_decision(
            request_id,
            _decision(True),
            expected_owner=claim_b.owner,
            expected_fence=claim_b.fence,
        )
    work = resume_storage(work)
    winner = new_proof_ledger(work)
    row = work.get(request_id)
    assert row is not None
    with execution_scope(_SCOPE):
        winner.complete(
            request_id,
            {"winner": "B"},
            _expected_owner=row.owner,
            _expected_fence=row.fence,
        )
    failures.extend(
        assert_effect_protocol_invariants(work, label="fence-crash/after-winner-complete")
    )
    if not any(item.startswith("fence-crash/after-winner-complete") for item in failures):
        decisions.append("fence-crash/after-winner-complete: invariants held")

    return failures, decisions


def run_expired_unknown_hard_block_sweeps() -> tuple[list[str], list[str]]:
    """UNKNOWN + blind class stays fail-closed across crash/resume redispatch."""
    from mycelium.action_ledger import LedgerHardBlockError

    failures: list[str] = []
    decisions: list[str] = []
    binding = standard_proof_binding()
    request_id = "proof-unknown-block"
    kwargs = {"amount": 1, "tool_call_id": "proof-unknown-block", "request_id": request_id}

    storage = InMemoryLedgerStorage()
    ctx = _RunCtx(
        storage=storage,
        request_id=request_id,
        kwargs=kwargs,
        binding=binding,
    )
    for step in ("claim", "decision_allow", "mark_unknown"):
        _STEP_RUNNERS[step](ctx)  # type: ignore[arg-type]
        storage = resume_storage(storage)
        ctx.storage = storage

    ledger = new_proof_ledger(storage)
    with execution_scope(_SCOPE):
        try:
            ledger.claim_side_effecting(
                request_id, PROOF_TOOL, (), dict(kwargs), binding
            )
            failures.append("unknown-block: redispatch after UNKNOWN did not hard-block")
        except LedgerHardBlockError:
            decisions.append("unknown-block: crash-resume redispatch stayed fail-closed")

    failures.extend(assert_effect_protocol_invariants(storage, label="unknown-block"))
    return failures, decisions
