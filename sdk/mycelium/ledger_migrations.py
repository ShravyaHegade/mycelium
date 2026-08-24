"""Explicit, idempotent migrations for durable ActionLedger rows."""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, replace
from typing import Protocol

from mycelium.action_ledger import LEDGER_ENTRY_SCHEMA_VERSION, LedgerEntry
from mycelium.transition import TerminalOutcome


class LedgerMigrationError(Exception):
    """Raised when a ledger migration cannot be planned or applied safely."""


class LedgerMigrationStorage(Protocol):
    def get(self, request_id: str) -> LedgerEntry | None: ...

    def set(self, entry: LedgerEntry) -> None: ...

    def list_all(self) -> list[LedgerEntry]: ...


@dataclass(frozen=True)
class LedgerMigrationPlan:
    target_version: int
    total_entries: int
    current_entries: int
    pending_entries: int
    active_pending_entries: int
    version_counts: dict[int, int]
    unsupported_versions: tuple[int, ...] = ()

    @property
    def can_apply(self) -> bool:
        return not self.unsupported_versions

    def to_dict(self) -> dict[str, object]:
        return {
            "target_version": self.target_version,
            "total_entries": self.total_entries,
            "current_entries": self.current_entries,
            "pending_entries": self.pending_entries,
            "active_pending_entries": self.active_pending_entries,
            "version_counts": {
                str(version): count for version, count in sorted(self.version_counts.items())
            },
            "unsupported_versions": list(self.unsupported_versions),
            "can_apply": self.can_apply,
        }


@dataclass(frozen=True)
class LedgerMigrationResult:
    target_version: int
    migrated_entries: int
    unchanged_entries: int

    def to_dict(self) -> dict[str, int]:
        return {
            "target_version": self.target_version,
            "migrated_entries": self.migrated_entries,
            "unchanged_entries": self.unchanged_entries,
        }


def _validated_target(target_version: int) -> int:
    if target_version != LEDGER_ENTRY_SCHEMA_VERSION:
        if target_version < LEDGER_ENTRY_SCHEMA_VERSION:
            raise LedgerMigrationError(
                "ledger downgrades are not supported; restore the pre-migration backup "
                "with the older Mycelium version instead"
            )
        raise LedgerMigrationError(
            f"target schema {target_version} is newer than this runtime supports "
            f"({LEDGER_ENTRY_SCHEMA_VERSION})"
        )
    return target_version


def _entry_version(entry: LedgerEntry) -> int:
    version = int(entry.schema_version)
    if version < 1:
        raise LedgerMigrationError(
            f"entry {entry.request_id!r} has invalid schema_version {version}"
        )
    return version


def _upgrade_v1_to_v2(entry: LedgerEntry) -> LedgerEntry:
    # Schema 1 predates durable effect identity. Its only safe identity is the
    # physical request id, which is also LedgerEntry.from_dict's compatibility
    # behavior. Preserve any partially populated values.
    effect_id = str(entry.effect_id or entry.request_id)
    aliases = tuple(dict.fromkeys((*entry.request_id_aliases, entry.request_id)))
    return replace(
        entry,
        effect_id=effect_id,
        request_id_aliases=aliases,
        schema_version=2,
    )


_UPGRADES = {1: _upgrade_v1_to_v2}


def upgrade_ledger_entry(
    entry: LedgerEntry,
    *,
    target_version: int = LEDGER_ENTRY_SCHEMA_VERSION,
) -> LedgerEntry:
    """Return ``entry`` upgraded to ``target_version`` without writing it."""

    target = _validated_target(target_version)
    version = _entry_version(entry)
    if version > target:
        raise LedgerMigrationError(
            f"entry {entry.request_id!r} uses unsupported future schema {version}"
        )
    upgraded = entry
    while version < target:
        migration = _UPGRADES.get(version)
        if migration is None:
            raise LedgerMigrationError(
                f"no ledger migration registered from schema {version} to {version + 1}"
            )
        upgraded = migration(upgraded)
        version = _entry_version(upgraded)
    return upgraded


def plan_ledger_migration(
    storage: LedgerMigrationStorage,
    *,
    target_version: int = LEDGER_ENTRY_SCHEMA_VERSION,
    now: float | None = None,
) -> LedgerMigrationPlan:
    """Inspect a ledger without modifying rows and return a migration plan."""

    target = _validated_target(target_version)
    observed_at = time.time() if now is None else now
    entries = storage.list_all()
    versions: Counter[int] = Counter()
    pending = 0
    active_pending = 0
    unsupported: set[int] = set()
    for entry in entries:
        version = _entry_version(entry)
        versions[version] += 1
        if version > target:
            unsupported.add(version)
            continue
        if version < target:
            upgrade_ledger_entry(entry, target_version=target)
            pending += 1
            if entry.resolved_terminal_outcome(now=observed_at) == TerminalOutcome.IN_FLIGHT:
                active_pending += 1
    return LedgerMigrationPlan(
        target_version=target,
        total_entries=len(entries),
        current_entries=versions.get(target, 0),
        pending_entries=pending,
        active_pending_entries=active_pending,
        version_counts=dict(versions),
        unsupported_versions=tuple(sorted(unsupported)),
    )


def apply_ledger_migration(
    storage: LedgerMigrationStorage,
    *,
    target_version: int = LEDGER_ENTRY_SCHEMA_VERSION,
    allow_active: bool = False,
) -> LedgerMigrationResult:
    """Upgrade every older row, refusing active or unsupported rows by default."""

    plan = plan_ledger_migration(storage, target_version=target_version)
    if not plan.can_apply:
        raise LedgerMigrationError(
            f"ledger contains unsupported schema versions {list(plan.unsupported_versions)}"
        )
    if plan.active_pending_entries and not allow_active:
        raise LedgerMigrationError(
            f"{plan.active_pending_entries} migration candidate(s) are IN_FLIGHT; "
            "stop workers first, then pass --allow-active only after confirming "
            "no worker can still write those rows"
        )

    migrated = 0
    unchanged = 0
    for entry in storage.list_all():
        if _entry_version(entry) == plan.target_version:
            unchanged += 1
            continue
        upgraded = upgrade_ledger_entry(entry, target_version=plan.target_version)
        storage.set(upgraded)
        stored = storage.get(entry.request_id)
        if stored is None or _entry_version(stored) != plan.target_version:
            raise LedgerMigrationError(
                f"migration write verification failed for entry {entry.request_id!r}"
            )
        migrated += 1
    return LedgerMigrationResult(
        target_version=plan.target_version,
        migrated_entries=migrated,
        unchanged_entries=unchanged,
    )


__all__ = [
    "LedgerMigrationError",
    "LedgerMigrationPlan",
    "LedgerMigrationResult",
    "LedgerMigrationStorage",
    "apply_ledger_migration",
    "plan_ledger_migration",
    "upgrade_ledger_entry",
]
