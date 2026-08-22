"""Randomized and enumerated effect-protocol property checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mycelium.action_ledger import InMemoryLedgerStorage
from mycelium.transition import ToolTransitionBinding
from mycelium.verify.proof.crash_sweep import _STEP_RUNNERS, StepKind, _RunCtx
from mycelium.verify.proof.harness import (
    assert_effect_protocol_invariants,
    resume_storage,
    standard_proof_binding,
)

LEGAL_PREFIXES: tuple[tuple[StepKind, ...], ...] = (
    ("claim",),
    ("claim", "decision_allow"),
    ("claim", "decision_deny"),
    ("claim", "decision_allow", "complete"),
    ("claim", "decision_allow", "fail_before"),
    ("claim", "decision_allow", "mark_unknown"),
    ("claim", "decision_allow", "advance_maybe"),
    ("claim", "decision_allow", "advance_maybe", "advance_crossed"),
    ("claim", "decision_allow", "advance_maybe", "advance_crossed", "complete"),
)


@dataclass(frozen=True)
class PropertyCase:
    name: str
    request_id: str
    tool_call_id: str
    steps: tuple[StepKind, ...]
    resume_every_step: bool


def enumerate_property_cases(*, limit: int | None = None) -> list[PropertyCase]:
    """Finite enumeration of legal step prefixes with optional per-step resume."""
    cases: list[PropertyCase] = []
    for index, steps in enumerate(LEGAL_PREFIXES):
        request_id = f"proof-prop-{index}"
        cases.append(
            PropertyCase(
                name=f"linear/{'-'.join(steps)}",
                request_id=request_id,
                tool_call_id=f"proof-prop-call-{index}",
                steps=steps,
                resume_every_step=False,
            )
        )
        cases.append(
            PropertyCase(
                name=f"resume/{'-'.join(steps)}",
                request_id=f"{request_id}-resume",
                tool_call_id=f"proof-prop-call-{index}-r",
                steps=steps,
                resume_every_step=True,
            )
        )
    if limit is not None:
        return cases[:limit]
    return cases


def run_property_case(
    case: PropertyCase,
    *,
    binding: ToolTransitionBinding | None = None,
) -> list[str]:
    binding = binding or standard_proof_binding()
    kwargs: dict[str, Any] = {
        "amount": 1,
        "tool_call_id": case.tool_call_id,
        "request_id": case.request_id,
    }
    ctx = _RunCtx(
        storage=InMemoryLedgerStorage(),
        request_id=case.request_id,
        kwargs=kwargs,
        binding=binding,
    )
    for step in case.steps:
        _STEP_RUNNERS[step](ctx)
        if case.resume_every_step:
            ctx.storage = resume_storage(ctx.storage)

    return assert_effect_protocol_invariants(ctx.storage, label=case.name)


def run_enumerated_properties() -> tuple[list[str], list[str]]:
    failures: list[str] = []
    decisions: list[str] = []
    for case in enumerate_property_cases():
        case_failures = run_property_case(case)
        failures.extend(case_failures)
        if not case_failures:
            decisions.append(f"{case.name}: invariants held")
    decisions.append(f"enumerated property cases: {len(enumerate_property_cases())}")
    return failures, decisions
