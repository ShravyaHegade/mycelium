"""Deterministic invariant checks for the effect-commit protocol.

The moat: a reimplementer has to rebuild the simulation suite from scratch. The
deterministic core is the *at-most-one-COMMITTED* invariant — for every
effect_id, at most one COMPLETED provider-side effect ever exists. The empirical
scenarios (timing-dependent, real-process crashes) feed their observed ledger
entries and provider effect logs into these pure, deterministic checks, which
turn "we ran a scenario" into "the invariant held".
"""

from __future__ import annotations

from collections import defaultdict
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

    An entry maps to an effect_id via its ``external_operation_ref`` (recorded
    by :func:`mycelium.record_external_operation` inside the side-effect
    boundary). Entries without a ref cannot be attributed to a provider effect
    and are ignored — the invariant is per provider-side effect, not per call.
    """
    committed: dict[str, list[str]] = defaultdict(list)
    for entry in entries:
        ref = getattr(entry, "external_operation_ref", None)
        if not ref:
            continue
        try:
            outcome = entry.resolved_terminal_outcome()
        except Exception:
            outcome = TerminalOutcome(entry.terminal_outcome)
        if outcome == TerminalOutcome.COMPLETED:
            committed[str(ref)].append(entry.request_id)
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
    provider_effect_ids: Sequence[str],
) -> tuple[list[InvariantViolation], list[str]]:
    """Cross-check the provider effect log against committed ledger entries.

    Returns ``(violations, warnings)``. A provider effect with more than one
    COMPLETED entry is a hard violation (duplicate provider-side effect). A
    provider effect with no COMPLETED entry is a warning, not a violation:
    the effect may be parked as ``UNKNOWN`` awaiting reconciliation, and
    at-most-once still holds (zero committed).
    """
    committed = committed_effect_ids(entries)
    violations: list[InvariantViolation] = []
    warnings: list[str] = []
    for raw in provider_effect_ids:
        effect_id = str(raw)
        rids = committed.get(effect_id, [])
        if len(rids) > 1:
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
