"""Deterministic invariant checks for the effect-commit protocol.

The moat: a reimplementer has to rebuild the simulation suite from scratch. The
deterministic core is the *at-most-one-COMMITTED* invariant — for every
effect_id, at most one COMPLETED provider-side effect ever exists. The empirical
scenarios (timing-dependent, real-process crashes) feed their observed ledger
entries and provider effect logs into these pure, deterministic checks, which
turn "we ran a scenario" into "the invariant held".
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from mycelium.transition import TerminalOutcome

__all__ = [
    "InvariantViolation",
    "check_at_most_one_committed",
    "check_provider_mapping",
    "committed_effect_ids",
]


@dataclass(frozen=True)
class InvariantViolation:
    """A single violation of the at-most-one-COMMITTED invariant."""

    effect_id: str
    message: str


def committed_effect_ids(entries: Iterable[Any]) -> dict[str, list[str]]:
    """Map effect_id -> [request_ids] for every COMPLETED ledger entry.

    The effect identity is the stable transition identity, never the provider's
    per-attempt operation handle. This lets different provider IDs produced by
    duplicate executions collide in the same invariant bucket.
    """
    committed: dict[str, list[str]] = defaultdict(list)
    for entry in entries:
        effect_id = (
            getattr(entry, "transition_key", None)
            or getattr(entry, "idempotency_key", None)
            or getattr(entry, "request_id", None)
        )
        if not effect_id:
            continue
        try:
            outcome = entry.resolved_terminal_outcome()
        except Exception:
            outcome = TerminalOutcome(entry.terminal_outcome)
        if outcome == TerminalOutcome.COMPLETED:
            committed[str(effect_id)].append(entry.request_id)
    return dict(committed)


def check_at_most_one_committed(
    entries: Iterable[Any],
) -> list[InvariantViolation]:
    """Assert: for every effect_id, at most one COMPLETED ledger entry.

    This is the core effect-commit invariant. A violation means two ledger rows
    committed the same provider-side effect — exactly the double-execution the
    protocol exists to prevent.
    """
    violations: list[InvariantViolation] = []
    for ref, rids in committed_effect_ids(entries).items():
        if len(rids) > 1:
            violations.append(
                InvariantViolation(
                    ref,
                    f"effect {ref!r} committed {len(rids)} times: {sorted(rids)}",
                )
            )
    return violations


def check_provider_mapping(
    entries: Iterable[Any],
    provider_effect_ids: Sequence[str | tuple[str, str]],
) -> tuple[list[InvariantViolation], list[str]]:
    """Cross-check the provider effect log against committed ledger entries.

    Returns ``(violations, warnings)``. A provider effect with more than one
    COMPLETED entry is a hard violation (duplicate provider-side effect). A
    provider effect with no COMPLETED entry is a warning, not a violation:
    the effect may be parked as ``UNKNOWN`` awaiting reconciliation, and
    at-most-once still holds (zero committed).
    """
    entry_list = list(entries)
    committed = committed_effect_ids(entry_list)
    ref_to_effect = {
        str(entry.external_operation_ref): str(
            getattr(entry, "transition_key", None)
            or getattr(entry, "idempotency_key", None)
            or entry.request_id
        )
        for entry in entry_list
        if getattr(entry, "external_operation_ref", None)
    }
    violations: list[InvariantViolation] = []
    warnings: list[str] = []
    executions: list[tuple[str, str]] = []
    for raw in provider_effect_ids:
        if isinstance(raw, tuple):
            effect_id, provider_id = map(str, raw)
        else:
            provider_id = str(raw)
            effect_id = ref_to_effect.get(provider_id, provider_id)
        executions.append((effect_id, provider_id))
    execution_counts = Counter(item[0] for item in executions)
    for effect_id, count in execution_counts.items():
        if count > 1:
            provider_ids = sorted(item[1] for item in executions if item[0] == effect_id)
            violations.append(
                InvariantViolation(
                    effect_id,
                    f"effect {effect_id!r} executed by provider {count} times: {provider_ids}",
                )
            )
    for effect_id in dict.fromkeys(item[0] for item in executions):
        rids = committed.get(effect_id, [])
        if len(rids) > 1 and execution_counts[effect_id] <= 1:
            violations.append(
                InvariantViolation(
                    effect_id,
                    f"provider effect {effect_id!r} recorded but committed "
                    f"{len(rids)} times: {sorted(rids)}",
                )
            )
        elif not rids:
            warnings.append(
                f"provider effect {effect_id!r} has no COMPLETED ledger entry "
                "(parked UNKNOWN / awaiting reconciliation)"
            )
    return violations, warnings
