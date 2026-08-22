"""Shared helpers for deterministic effect-protocol proof runs."""

from __future__ import annotations

from typing import Any

from mycelium.action_ledger import ActionLedger, InMemoryLedgerStorage, LedgerEntry
from mycelium.transition import (
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
)
from mycelium.verify.invariants import (
    check_at_most_one_committed,
    check_at_most_one_committed_effect_state,
    check_effect_state_consistency,
    check_unique_effect_id_index,
)

PROOF_TOOL = "verify_effect_protocol"


def standard_proof_scope() -> TransitionScope:
    return TransitionScope(thread_id="proof", run_id="proof")


def standard_proof_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="mycelium-proof",
        policy_version="proof",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def idempotent_reclaim_binding() -> ToolTransitionBinding:
    """Binding that allows EXPIRED/not_crossed reclaim (fence-takeover proofs)."""
    return ToolTransitionBinding.for_tool(
        agent_id="mycelium-proof",
        policy_version="proof",
        side_effect_class=SideEffectClass.IDEMPOTENT_MUTATE,
    )


def new_proof_ledger(
    storage: InMemoryLedgerStorage,
    *,
    reconciler: Any | None = None,
    lease_ttl: float = 30.0,
) -> ActionLedger:
    return ActionLedger(
        storage=storage,
        reconciler=reconciler,
        lease_ttl=lease_ttl,
        lease_renew_interval=0,
        poll_interval=0.001,
        poll_timeout=0.05,
        reclaim_requires_death_signal=False,
    )


def resume_storage(storage: InMemoryLedgerStorage) -> InMemoryLedgerStorage:
    """Simulate a process restart: fresh client, durable rows preserved."""
    resumed = InMemoryLedgerStorage()
    for entry in storage.list_all():
        resumed.set(LedgerEntry.from_dict(entry.to_dict()))
    return resumed


def assert_effect_protocol_invariants(
    storage: InMemoryLedgerStorage,
    *,
    label: str,
) -> list[str]:
    """Return human-readable failures for the core effect-commit invariant set."""
    entries = storage.list_all()
    failures: list[str] = []
    for violation in check_at_most_one_committed(entries):
        failures.append(f"{label}: {violation.message}")
    for violation in check_at_most_one_committed_effect_state(entries):
        failures.append(f"{label}: {violation.message}")
    for violation in check_effect_state_consistency(entries):
        failures.append(f"{label}: {violation.message}")
    for violation in check_unique_effect_id_index(entries):
        failures.append(f"{label}: {violation.message}")
    return failures
