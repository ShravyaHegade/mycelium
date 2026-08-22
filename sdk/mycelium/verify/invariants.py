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

from mycelium.transition import EffectState, TerminalOutcome, resolve_effect_state

__all__ = [
    "InvariantViolation",
    "check_at_most_one_committed",
    "check_at_most_one_committed_effect_state",
    "check_effect_state_consistency",
    "check_unique_effect_id_index",
    "check_provider_mapping",
    "committed_effect_ids",
    "committed_effect_ids_by_state",
]


@dataclass(frozen=True)
class InvariantViolation:
    """A single violation of the at-most-one-COMMITTED invariant."""

    effect_id: str
    message: str


def _effect_ref(entry: Any) -> str | None:
    """The dedup identity to bucket a committed row under.

    ``effect_id`` is the authoritative cross-request dedup identity. Legacy
    rows missing ``effect_id`` fall back to ``request_id``.
    """
    return getattr(entry, "effect_id", None) or getattr(entry, "request_id", None)


def committed_effect_ids(entries: Iterable[Any]) -> dict[str, list[str]]:
    """Map effect_id -> [request_ids] for every COMPLETED ledger entry.

    The effect identity is the stable transition identity, never the provider's
    per-attempt operation handle. This lets different provider IDs produced by
    duplicate executions collide in the same invariant bucket.
    """
    committed: dict[str, list[str]] = defaultdict(list)
    for entry in entries:
        effect_id = _effect_ref(entry)
        if not effect_id:
            continue
        try:
            outcome = entry.resolved_terminal_outcome()
        except Exception:
            outcome = TerminalOutcome(entry.terminal_outcome)
        if outcome == TerminalOutcome.COMPLETED:
            committed[str(effect_id)].append(entry.request_id)
    return dict(committed)


def committed_effect_ids_by_state(entries: Iterable[Any]) -> dict[str, list[str]]:
    """Same as :func:`committed_effect_ids`, but on the unified ``EffectState``.

    Groups by ``resolve_effect_state(entry) == EffectState.COMMITTED`` instead
    of ``resolved_terminal_outcome() == TerminalOutcome.COMPLETED``. The two
    should always agree (``COMMITTED`` is defined as the unconditional image
    of legacy ``COMPLETED``) — :func:`check_effect_state_consistency` asserts
    that. Kept as a separate function (rather than folding into
    :func:`committed_effect_ids`) so a divergence between the two views is
    itself a detectable, reportable invariant violation.
    """
    committed: dict[str, list[str]] = defaultdict(list)
    for entry in entries:
        effect_id = _effect_ref(entry)
        if not effect_id:
            continue
        if resolve_effect_state(entry) == EffectState.COMMITTED:
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


def check_at_most_one_committed_effect_state(
    entries: Iterable[Any],
) -> list[InvariantViolation]:
    """Same invariant as :func:`check_at_most_one_committed`, on ``EffectState``.

    Asserts the at-most-one-COMMITTED invariant holds under the unified WAL
    intent, not just the legacy ``terminal_outcome`` read path — a
    reimplementer following only ``EffectState`` must get the same guarantee.
    """
    violations: list[InvariantViolation] = []
    for ref, rids in committed_effect_ids_by_state(entries).items():
        if len(rids) > 1:
            violations.append(
                InvariantViolation(
                    ref,
                    f"effect {ref!r} EffectState.COMMITTED {len(rids)} times: {sorted(rids)}",
                )
            )
    return violations


def check_effect_state_consistency(entries: Iterable[Any]) -> list[InvariantViolation]:
    """Assert ``resolve_effect_state`` and ``resolved_terminal_outcome`` agree.

    ``EffectState.COMMITTED`` is defined as the unconditional image of legacy
    ``TerminalOutcome.COMPLETED`` (see :func:`mycelium.transition.
    resolve_effect_state`); this check catches any future edit that breaks
    that equivalence for a live row before it silently causes a divergent
    at-most-one-COMMITTED verdict between the two invariant families above.
    """
    violations: list[InvariantViolation] = []
    for entry in entries:
        try:
            terminal = entry.resolved_terminal_outcome()
        except Exception:
            terminal = TerminalOutcome(entry.terminal_outcome)
        state = resolve_effect_state(entry)
        is_completed = terminal == TerminalOutcome.COMPLETED
        is_committed = state == EffectState.COMMITTED
        if is_completed != is_committed:
            effect_id = str(
                getattr(entry, "effect_id", None)
                or getattr(entry, "request_id", None)
                or "?"
            )
            violations.append(
                InvariantViolation(
                    effect_id,
                    f"request {entry.request_id!r}: terminal_outcome={terminal.value!r} "
                    f"(completed={is_completed}) disagrees with "
                    f"EffectState={state.value!r} (committed={is_committed})",
                )
            )
    return violations


def check_unique_effect_id_index(entries: Iterable[Any]) -> list[InvariantViolation]:
    """Assert every ``effect_id`` resolves to exactly one canonical request row."""
    mapping: dict[str, set[str]] = defaultdict(set)
    for entry in entries:
        effect_id = str(getattr(entry, "effect_id", None) or getattr(entry, "request_id", ""))
        request_id = str(getattr(entry, "request_id", ""))
        if not effect_id or not request_id:
            continue
        mapping[effect_id].add(request_id)
    violations: list[InvariantViolation] = []
    for effect_id, request_ids in mapping.items():
        if len(request_ids) > 1:
            violations.append(
                InvariantViolation(
                    effect_id,
                    f"effect_id {effect_id!r} points to multiple request_ids: "
                    f"{sorted(request_ids)}",
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
        str(entry.external_operation_ref): str(_effect_ref(entry) or entry.request_id)
        for entry in entry_list
        if getattr(entry, "external_operation_ref", None)
    }
    ledger_key_to_effect: dict[str, str] = {}
    for entry in entry_list:
        effect = str(_effect_ref(entry) or entry.request_id)
        ledger_key_to_effect[entry.request_id] = effect
        for alias in getattr(entry, "request_id_aliases", ()) or ():
            if alias:
                ledger_key_to_effect[str(alias)] = effect
    violations: list[InvariantViolation] = []
    warnings: list[str] = []
    executions: list[tuple[str, str]] = []
    for raw in provider_effect_ids:
        if isinstance(raw, tuple):
            ledger_key, provider_id = map(str, raw)
            effect_id = ledger_key_to_effect.get(ledger_key, ledger_key)
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
