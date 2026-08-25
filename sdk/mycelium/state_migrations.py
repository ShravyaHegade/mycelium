"""Copy legacy guard state into the unified atomic state backend."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from mycelium.audit_receipt import AtomicAuditReceiptStorage, AuditReceiptRecord
from mycelium.completion_contract import AtomicCompletionStorage, CompletionRunState
from mycelium.config import MyceliumConfig
from mycelium.loop_guard import AtomicLoopGuardStorage, LoopRunState
from mycelium.scope_guard import AtomicScopeGuardStorage, ScopeRunState
from mycelium.state_flush import AtomicStateFlushStorage, StateSnapshot
from mycelium.storage.atomic_state import AtomicStateBackend


class StateMigrationError(RuntimeError):
    """Raised when guard state cannot be copied without losing data."""


@dataclass(frozen=True)
class StateMigrationPlan:
    total_records: int
    pending_records: int
    unchanged_records: int
    conflicting_records: int
    feature_counts: dict[str, int]

    @property
    def can_apply(self) -> bool:
        return self.conflicting_records == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_records": self.total_records,
            "pending_records": self.pending_records,
            "unchanged_records": self.unchanged_records,
            "conflicting_records": self.conflicting_records,
            "feature_counts": dict(sorted(self.feature_counts.items())),
            "can_apply": self.can_apply,
        }


@dataclass(frozen=True)
class StateMigrationResult:
    migrated_records: int
    unchanged_records: int

    def to_dict(self) -> dict[str, int]:
        return {
            "migrated_records": self.migrated_records,
            "unchanged_records": self.unchanged_records,
        }


@dataclass(frozen=True)
class _StateRecord:
    feature: str
    key: str
    payload: dict[str, Any]


def _destination(cfg: MyceliumConfig) -> tuple[AtomicStateBackend, str]:
    backend = cfg.build_state_backend()
    if backend is None:
        raise StateMigrationError("state migration requires a top-level state_backend")
    return backend, str((cfg.state_backend or {}).get("namespace", "mycelium"))


def _already_shared(cfg: MyceliumConfig, raw: dict[str, Any]) -> bool:
    storage = raw.get("storage")
    return storage == "shared" or (storage is None and cfg.state_backend is not None)


def _records(cfg: MyceliumConfig) -> Iterable[_StateRecord]:
    sections: tuple[tuple[str, dict[str, Any] | None], ...] = (
        ("loop_guard", cfg.loop_guard),
        ("scope_guard", cfg.scope_guard),
        ("completion", cfg.completion),
        ("state_flush", cfg.state_flush),
        ("audit_receipt", cfg.audit_receipt),
    )
    for feature, raw in sections:
        if raw is None or _already_shared(cfg, raw):
            continue
        atomic = cfg._guard_atomic_backend(raw)
        if feature == "loop_guard":
            storage = (
                AtomicLoopGuardStorage(atomic[0], namespace=f"{atomic[1]}:{feature}")
                if atomic is not None
                else cfg._build_loop_guard_storage(raw)
            )
            entries: Iterable[LoopRunState] = storage.list_all()
            for entry in entries:
                yield _StateRecord(feature, entry.scope_key, entry.to_dict())
        elif feature == "scope_guard":
            storage = (
                AtomicScopeGuardStorage(atomic[0], namespace=f"{atomic[1]}:{feature}")
                if atomic is not None
                else cfg._build_scope_guard_storage(raw)
            )
            entries_scope: Iterable[ScopeRunState] = storage.list_all()
            for entry in entries_scope:
                yield _StateRecord(feature, entry.scope_key, entry.to_dict())
        elif feature == "completion":
            storage = (
                AtomicCompletionStorage(atomic[0], namespace=f"{atomic[1]}:{feature}")
                if atomic is not None
                else cfg._build_completion_storage(raw)
            )
            entries_completion: Iterable[CompletionRunState] = storage.list_all()
            for entry in entries_completion:
                yield _StateRecord(feature, entry.scope_key, entry.to_dict())
        elif feature == "state_flush":
            storage = (
                AtomicStateFlushStorage(atomic[0], namespace=f"{atomic[1]}:{feature}")
                if atomic is not None
                else cfg._build_state_flush_storage(raw)
            )
            entries_flush: Iterable[StateSnapshot] = storage.list_all()
            for entry in entries_flush:
                yield _StateRecord(feature, entry.run_id, entry.to_dict())
        else:
            storage = (
                AtomicAuditReceiptStorage(atomic[0], namespace=f"{atomic[1]}:{feature}")
                if atomic is not None
                else cfg._build_audit_receipt_storage(raw)
            )
            entries_audit: Iterable[AuditReceiptRecord] = storage.list_all()
            for entry in entries_audit:
                yield _StateRecord(feature, entry.receipt_id, entry.to_dict())


def plan_state_migration(cfg: MyceliumConfig) -> StateMigrationPlan:
    """Inspect legacy guard state and the destination without changing either."""

    backend, base = _destination(cfg)
    pending = unchanged = conflicts = 0
    feature_counts: dict[str, int] = {}
    records = list(_records(cfg))
    for record in records:
        feature_counts[record.feature] = feature_counts.get(record.feature, 0) + 1
        existing = backend.get(f"{base}:{record.feature}", record.key)
        if existing is None:
            pending += 1
        elif existing.value == record.payload:
            unchanged += 1
        else:
            conflicts += 1
    return StateMigrationPlan(
        total_records=len(records),
        pending_records=pending,
        unchanged_records=unchanged,
        conflicting_records=conflicts,
        feature_counts=feature_counts,
    )


def apply_state_migration(cfg: MyceliumConfig) -> StateMigrationResult:
    """Copy source records, refusing to overwrite different destination state."""

    plan = plan_state_migration(cfg)
    if not plan.can_apply:
        raise StateMigrationError(
            f"state migration found {plan.conflicting_records} conflicting destination record(s)"
        )
    backend, base = _destination(cfg)
    migrated = unchanged = 0
    for record in _records(cfg):
        namespace = f"{base}:{record.feature}"
        if backend.create(namespace, record.key, record.payload):
            migrated += 1
            continue
        existing = backend.get(namespace, record.key)
        if existing is not None and existing.value == record.payload:
            unchanged += 1
            continue
        raise StateMigrationError(
            f"destination changed during migration for {record.feature}/{record.key}"
        )
    return StateMigrationResult(migrated_records=migrated, unchanged_records=unchanged)


__all__ = [
    "StateMigrationError",
    "StateMigrationPlan",
    "StateMigrationResult",
    "apply_state_migration",
    "plan_state_migration",
]
