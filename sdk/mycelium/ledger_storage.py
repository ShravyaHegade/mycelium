"""Storage backends for the Mycelium action ledger."""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

from mycelium.ledger_model import (
    DEFAULT_LEASE_TTL,
    LedgerEntry,
    LedgerError,
    LedgerStorageUnavailableError,
)
from mycelium.storage._helpers import (
    claim_inflight_outcome,
    default_try_claim_inflight,
    lease_allows_renew,
    with_lease,
)
from mycelium.storage.json_file import LockedJsonDictFile
from mycelium.storage.transition_query import (
    TransitionPage,
    decode_cursor,
    encode_cursor,
    entry_sort_key,
)

__all__ = [
    "LedgerStorage",
    "InMemoryLedgerStorage",
    "FileLedgerStorage",
]


@contextmanager
def _storage_errors(operation: str) -> Iterator[None]:
    """Re-raise backend storage failures as :class:`LedgerStorageUnavailableError`.

    Only wraps exceptions raised by the storage layer itself — ``LedgerError``
    subclasses (policy refusals, hard blocks) pass through unchanged, and tool
    exceptions never reach this boundary (the claim path never runs tool code).
    The backend exception is preserved as ``__cause__``.
    """
    try:
        yield
    except LedgerError:
        raise
    except Exception as exc:
        raise LedgerStorageUnavailableError(
            f"ledger storage unavailable during {operation}: {type(exc).__name__}: {exc}"
        ) from exc


class LedgerStorage:
    """Backend interface for durable action ledger records."""

    def get(self, request_id: str) -> LedgerEntry | None:
        """Return the entry for request_id, or None if not found."""
        raise NotImplementedError

    def set(self, entry: LedgerEntry) -> None:
        """Persist entry, replacing any existing entry with the same request_id."""
        raise NotImplementedError

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ) -> tuple[str, LedgerEntry | None]:
        """Atomically claim an in-flight entry.

        Returns ``("claimed", None)``, ``("completed", entry)``, or
        ``("in_flight", entry)``. Redis/Postgres backends override with
        atomic primitives; file storage uses an exclusive lock.
        """
        return default_try_claim_inflight(
            self,
            entry,
            lease_ttl=lease_ttl,
        )

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        """Atomically write *entry* only if the stored entry's terminal outcome
        is one of *expected_terminal_outcomes* (and *expected_owner* matches,
        when set).

        When ``expected_fence`` is set, also refuse unless the stored entry's
        fence equals it (Kleppmann fencing — a superseded worker holds a lower
        fence and is rejected here regardless of its lease clock).

        When ``require_lease_held_at`` is set, also refuse if the stored lease
        is already expired at that timestamp (renew path — closes TOCTOU
        between get and write).

        Returns ``True`` when the write succeeds, ``False`` when the pre-condition
        is not met (caller raises ``LedgerOutcomeAlreadySetError``).

        When ``expected_effect_state`` is set, compare the stored
        ``effect_phase`` against that unified ``EffectState`` member string.

        The default implementation performs a get+set (single-process only).
        Override with an atomic compare-and-swap for multi-process backends.
        """
        existing = self.get(entry.request_id)
        if existing is None:
            return False
        if existing.terminal_outcome not in expected_terminal_outcomes:
            return False
        if expected_owner is not None and existing.owner != expected_owner:
            return False
        if expected_fence is not None and existing.fence != expected_fence:
            return False
        if expected_effect_state is not None and existing.effect_phase != expected_effect_state:
            return False
        if require_lease_held_at is not None and not lease_allows_renew(
            existing.lease_until, now=require_lease_held_at
        ):
            return False
        self.set(entry)
        return True

    def list_all(self) -> list[LedgerEntry]:
        """Return all entries. Intended for debugging/auditing only."""
        raise NotImplementedError

    def list_page(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        tool: str | None = None,
        outcome: str | None = None,
        parent_request_id: str | None = None,
        started_after: float | None = None,
        started_before: float | None = None,
        finished_before: float | None = None,
    ) -> TransitionPage[LedgerEntry]:
        """Return a stable page; durable backends override with server-side queries."""
        decoded = decode_cursor(cursor)
        entries = sorted(self.list_all(), key=entry_sort_key)
        selected: list[LedgerEntry] = []
        for entry in entries:
            if decoded is not None and entry_sort_key(entry) <= decoded:
                continue
            if tool is not None and entry.tool != tool:
                continue
            if outcome is not None and entry.terminal_outcome != outcome:
                continue
            if parent_request_id is not None and entry.parent_request_id != parent_request_id:
                continue
            if started_after is not None and entry.started_at < started_after:
                continue
            if started_before is not None and entry.started_at >= started_before:
                continue
            if finished_before is not None and (
                entry.finished_at is None or entry.finished_at >= finished_before
            ):
                continue
            selected.append(entry)
            if len(selected) > limit:
                break
        page_entries = selected[:limit]
        next_cursor = None
        if len(selected) > limit and page_entries:
            next_cursor = encode_cursor(*entry_sort_key(page_entries[-1]))
        return TransitionPage(page_entries, next_cursor)

    def delete_entries(self, request_ids: list[str]) -> int:
        """Delete entries by id. Backends supporting retention must override."""
        raise NotImplementedError("this ledger backend does not support pruning")

    def resolve_request_id(self, effect_id: str) -> str | None:
        """Resolve ``effect_id`` to its canonical ``request_id``.

        Default implementation scans all rows (legacy-safe, deterministic).
        Backends with secondary indexes should override.
        """
        candidates: list[LedgerEntry] = []
        for entry in self.list_all():
            ref = str(getattr(entry, "effect_id", None) or entry.request_id)
            if ref == effect_id:
                candidates.append(entry)
        if not candidates:
            return None
        candidates.sort(
            key=lambda item: (float(getattr(item, "started_at", 0.0) or 0.0), item.request_id)
        )
        return candidates[0].request_id

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        """Return the canonical row for ``effect_id``, if present."""
        request_id = self.resolve_request_id(effect_id)
        if request_id is None:
            return None
        return self.get(request_id)


class InMemoryLedgerStorage(LedgerStorage):
    """Default in-memory storage. Survives within the process only.

    Thread-safe via ``_lock`` (``threading.RLock``) so concurrent in-process
    claims and transitions do not lose writes.  Multi-process users must
    choose a durable backend.
    """

    def __init__(self) -> None:
        self._entries: dict[str, LedgerEntry] = {}
        self._effect_index: dict[str, str] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _effect_ref(entry: LedgerEntry) -> str:
        return str(entry.effect_id or entry.request_id)

    def _resolve_effect_locked(self, effect_id: str) -> str | None:
        canonical = self._effect_index.get(effect_id)
        if canonical is not None:
            row = self._entries.get(canonical)
            if row is not None and self._effect_ref(row) == effect_id:
                return canonical
            self._effect_index.pop(effect_id, None)
        candidates = [row for row in self._entries.values() if self._effect_ref(row) == effect_id]
        if not candidates:
            return None
        candidates.sort(
            key=lambda item: (float(getattr(item, "started_at", 0.0) or 0.0), item.request_id)
        )
        canonical = candidates[0].request_id
        self._effect_index[effect_id] = canonical
        return canonical

    def get(self, request_id: str) -> LedgerEntry | None:
        with self._lock:
            return self._entries.get(request_id)

    def set(self, entry: LedgerEntry) -> None:
        with self._lock:
            effect_id = self._effect_ref(entry)
            canonical = self._resolve_effect_locked(effect_id)
            if (
                canonical is not None
                and canonical != entry.request_id
                and entry.request_id not in self._entries
            ):
                return
            self._entries[entry.request_id] = entry
            if canonical is None or canonical == entry.request_id:
                self._effect_index[effect_id] = entry.request_id

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ) -> tuple[str, LedgerEntry | None]:
        with self._lock:
            now = time.time()
            effect_id = self._effect_ref(entry)
            canonical = self._resolve_effect_locked(effect_id)
            active_request_id = canonical or entry.request_id
            existing = self._entries.get(active_request_id)
            outcome = claim_inflight_outcome(existing, now=now)
            if outcome == "completed":
                return "completed", existing
            if outcome == "in_flight":
                return "in_flight", existing
            claim_entry = (
                entry
                if active_request_id == entry.request_id
                else replace(entry, request_id=active_request_id)
            )
            leased = with_lease(claim_entry, now=now, lease_ttl=lease_ttl, prior=existing)
            self._entries[active_request_id] = leased
            self._effect_index[effect_id] = active_request_id
            return "claimed", None

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        with self._lock:
            existing = self._entries.get(entry.request_id)
            if existing is None:
                return False
            if existing.terminal_outcome not in expected_terminal_outcomes:
                return False
            if expected_owner is not None and existing.owner != expected_owner:
                return False
            if expected_fence is not None and existing.fence != expected_fence:
                return False
            if expected_effect_state is not None and existing.effect_phase != expected_effect_state:
                return False
            if require_lease_held_at is not None and not lease_allows_renew(
                existing.lease_until, now=require_lease_held_at
            ):
                return False
            # Route the write through set() so subclass hooks / failure
            # injection (and any future durability wrappers) still see it.
            # RLock allows re-entry from set() while we hold the CAS lock.
            self.set(entry)
            return True

    def list_all(self) -> list[LedgerEntry]:
        with self._lock:
            return list(self._entries.values())

    def delete_entries(self, request_ids: list[str]) -> int:
        with self._lock:
            deleted = 0
            for request_id in request_ids:
                entry = self._entries.pop(request_id, None)
                if entry is None:
                    continue
                deleted += 1
                effect_id = self._effect_ref(entry)
                if self._effect_index.get(effect_id) == request_id:
                    self._effect_index.pop(effect_id, None)
            return deleted

    def resolve_request_id(self, effect_id: str) -> str | None:
        with self._lock:
            return self._resolve_effect_locked(effect_id)

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        with self._lock:
            request_id = self._resolve_effect_locked(effect_id)
            if request_id is None:
                return None
            return self._entries.get(request_id)


class FileLedgerStorage(LedgerStorage):
    """JSON-file-backed storage with ``fcntl`` + threading locking.

    The ``fcntl`` lock guards across processes; the ``threading.Lock`` guards
    across threads within the same process (``flock`` has process-level
    semantics on macOS/Linux, so multiple threads cannot rely on it alone).
    """

    def __init__(self, path: str | Path) -> None:
        ledger_path = Path(path)
        self._file = LockedJsonDictFile(ledger_path)
        self._effect_index_path = ledger_path.with_suffix(ledger_path.suffix + ".effect-index.json")
        self._lock = threading.Lock()

    @staticmethod
    def _effect_ref_from_raw(raw: dict[str, Any], request_id: str) -> str:
        return str(raw.get("effect_id") or request_id)

    @staticmethod
    def _started_at_from_raw(raw: dict[str, Any]) -> float:
        value = raw.get("started_at")
        try:
            return float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _load_effect_index_unlocked(self) -> dict[str, str]:
        if not self._effect_index_path.exists():
            return {}
        try:
            with self._effect_index_path.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(loaded, dict):
            return {}
        index: dict[str, str] = {}
        for effect_id, request_id in loaded.items():
            if (
                isinstance(effect_id, str)
                and effect_id
                and isinstance(request_id, str)
                and request_id
            ):
                index[effect_id] = request_id
        return index

    def _save_effect_index_unlocked(self, index: dict[str, str]) -> None:
        tmp = self._effect_index_path.with_suffix(self._effect_index_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(index, handle, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self._effect_index_path)
        try:
            dir_fd = os.open(str(self._effect_index_path.parent), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    def _resolve_effect_locked(
        self,
        data: dict[str, dict[str, Any]],
        index: dict[str, str],
        effect_id: str,
    ) -> tuple[str | None, bool]:
        dirty = False
        canonical = index.get(effect_id)
        if canonical is not None:
            raw = data.get(canonical)
            if raw is not None and self._effect_ref_from_raw(raw, canonical) == effect_id:
                return canonical, False
            index.pop(effect_id, None)
            dirty = True
        candidates: list[tuple[float, str]] = []
        for request_id, raw in data.items():
            if self._effect_ref_from_raw(raw, request_id) == effect_id:
                candidates.append((self._started_at_from_raw(raw), request_id))
        if not candidates:
            return None, dirty
        candidates.sort(key=lambda item: (item[0], item[1]))
        canonical = candidates[0][1]
        if index.get(effect_id) != canonical:
            index[effect_id] = canonical
            dirty = True
        return canonical, dirty

    def get(self, request_id: str) -> LedgerEntry | None:
        def read(data: dict[str, dict[str, Any]]) -> LedgerEntry | None:
            raw = data.get(request_id)
            if raw is None:
                return None
            return LedgerEntry.from_dict(raw)

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def set(self, entry: LedgerEntry) -> None:
        def mutate(data: dict[str, dict[str, Any]]) -> None:
            index = self._load_effect_index_unlocked()
            effect_id = str(entry.effect_id or entry.request_id)
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            if (
                canonical is not None
                and canonical != entry.request_id
                and entry.request_id not in data
            ):
                if dirty:
                    self._save_effect_index_unlocked(index)
                return
            data[entry.request_id] = entry.to_dict()
            if canonical is None or canonical == entry.request_id:
                if index.get(effect_id) != entry.request_id:
                    index[effect_id] = entry.request_id
                    dirty = True
            if dirty:
                self._save_effect_index_unlocked(index)

        with self._lock:
            self._file.read_modify_write(mutate)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ) -> tuple[str, LedgerEntry | None]:
        outcome: list[tuple[str, LedgerEntry | None]] = []

        def mutate(data: dict[str, dict[str, Any]]) -> None:
            index = self._load_effect_index_unlocked()
            effect_id = str(entry.effect_id or entry.request_id)
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            active_request_id = canonical or entry.request_id
            raw = data.get(active_request_id)
            existing = LedgerEntry.from_dict(raw) if raw is not None else None
            now = time.time()
            result = claim_inflight_outcome(existing, now=now)
            if result == "completed":
                if dirty:
                    self._save_effect_index_unlocked(index)
                outcome.append(("completed", existing))
                return
            if result == "in_flight":
                if dirty:
                    self._save_effect_index_unlocked(index)
                outcome.append(("in_flight", existing))
                return
            claim_entry = (
                entry
                if active_request_id == entry.request_id
                else replace(entry, request_id=active_request_id)
            )
            leased = with_lease(claim_entry, now=now, lease_ttl=lease_ttl, prior=existing)
            data[active_request_id] = leased.to_dict()
            if index.get(effect_id) != active_request_id:
                index[effect_id] = active_request_id
                dirty = True
            if dirty:
                self._save_effect_index_unlocked(index)
            outcome.append(("claimed", None))

        with self._lock:
            self._file.read_modify_write(mutate)
        return outcome[0]

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        result: list[bool] = []

        def mutate(data: dict[str, dict[str, Any]]) -> None:
            index = self._load_effect_index_unlocked()
            dirty = False
            raw = data.get(entry.request_id)
            if raw is None:
                result.append(False)
                return
            existing = LedgerEntry.from_dict(raw)
            if existing.terminal_outcome not in expected_terminal_outcomes:
                result.append(False)
                return
            if expected_owner is not None and existing.owner != expected_owner:
                result.append(False)
                return
            if expected_fence is not None and existing.fence != expected_fence:
                result.append(False)
                return
            if expected_effect_state is not None and existing.effect_phase != expected_effect_state:
                result.append(False)
                return
            if require_lease_held_at is not None and not lease_allows_renew(
                existing.lease_until, now=require_lease_held_at
            ):
                result.append(False)
                return
            data[entry.request_id] = entry.to_dict()
            effect_id = str(entry.effect_id or entry.request_id)
            canonical, canonical_dirty = self._resolve_effect_locked(data, index, effect_id)
            dirty = dirty or canonical_dirty
            if canonical is None or canonical == entry.request_id:
                if index.get(effect_id) != entry.request_id:
                    index[effect_id] = entry.request_id
                    dirty = True
            if dirty:
                self._save_effect_index_unlocked(index)
            result.append(True)

        with self._lock:
            self._file.read_modify_write(mutate)
        return result[0]

    def list_all(self) -> list[LedgerEntry]:
        def read(data: dict[str, dict[str, Any]]) -> list[LedgerEntry]:
            return [LedgerEntry.from_dict(raw) for raw in data.values()]

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def delete_entries(self, request_ids: list[str]) -> int:
        targets = set(request_ids)
        deleted: list[int] = []

        def mutate(data: dict[str, dict[str, Any]]) -> None:
            count = 0
            for request_id in targets:
                if data.pop(request_id, None) is not None:
                    count += 1
            index = self._load_effect_index_unlocked()
            stale = [effect_id for effect_id, request_id in index.items() if request_id in targets]
            for effect_id in stale:
                index.pop(effect_id, None)
            if stale:
                self._save_effect_index_unlocked(index)
            deleted.append(count)

        with self._lock:
            self._file.read_modify_write(mutate)
        return deleted[0]

    def resolve_request_id(self, effect_id: str) -> str | None:
        def read(data: dict[str, dict[str, Any]]) -> str | None:
            index = self._load_effect_index_unlocked()
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            if dirty:
                self._save_effect_index_unlocked(index)
            return canonical

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        def read(data: dict[str, dict[str, Any]]) -> LedgerEntry | None:
            index = self._load_effect_index_unlocked()
            canonical, dirty = self._resolve_effect_locked(data, index, effect_id)
            if dirty:
                self._save_effect_index_unlocked(index)
            if canonical is None:
                return None
            raw = data.get(canonical)
            if raw is None:
                return None
            return LedgerEntry.from_dict(raw)

        with self._lock:
            return self._file.read_modify_write_no_save(read)
