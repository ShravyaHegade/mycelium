"""ScopeGuard (AF-008): freeze the run tool allowlist; block mid-run widen.

Snapshots which tools a run may call (from YAML ``tools:`` / ``registry``)
and re-checks every dispatch so handoffs cannot silently add tools.
Entity/path/output stay on ``@bounded`` — this module is allowlist-only.
"""

from __future__ import annotations

import functools
import inspect
import threading
import time
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from mycelium.action_ledger import LedgerHardBlockError
from mycelium.loop_guard import (
    MISSING_RUN_ID_POLICIES,
    MISSING_RUN_ID_POLICY_WARN,
    enforce_run_identity,
    resolve_loop_scope_key,
)
from mycelium.storage.json_file import LockedJsonDictFile
from mycelium.tool_boundary import ToolBoundaryError

P = ParamSpec("P")
R = TypeVar("R")

ON_VIOLATION_SOFT = "soft"
ON_VIOLATION_HARD = "hard"
ON_VIOLATION_MODES = frozenset({ON_VIOLATION_SOFT, ON_VIOLATION_HARD})

VIOLATION_TOOL = "scope_escalation_tool"

_SCOPE_MISSING_WARNED = False


class ScopeWidenRefusedError(Exception):
    """Raised when a host tries to widen an already-frozen run allowlist."""


@dataclass(frozen=True)
class ScopeGrant:
    """Immutable tool allowlist frozen for a run."""

    allowed_tools: frozenset[str]

    def __post_init__(self) -> None:
        if not self.allowed_tools:
            raise ValueError("allowed_tools must be non-empty")

    def to_dict(self) -> dict[str, Any]:
        return {"allowed_tools": sorted(self.allowed_tools)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScopeGrant:
        return cls(
            allowed_tools=frozenset(str(t) for t in (data.get("allowed_tools") or []))
        )

    def is_at_most_as_permissive_as(self, other: ScopeGrant) -> bool:
        """True if ``self`` is equal to or a subset of ``other`` (no widen)."""
        return self.allowed_tools.issubset(other.allowed_tools)


@dataclass
class ScopeRunState:
    """Durable frozen allowlist for one run / thread scope key."""

    scope_key: str
    grant: ScopeGrant
    bound_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope_key": self.scope_key,
            "grant": self.grant.to_dict(),
            "bound_at": self.bound_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScopeRunState:
        return cls(
            scope_key=str(data["scope_key"]),
            grant=ScopeGrant.from_dict(data.get("grant") or {}),
            bound_at=float(data.get("bound_at") or time.time()),
            updated_at=float(data.get("updated_at") or time.time()),
        )


class ScopeGuardStorage:
    """Storage protocol for per-run frozen grants."""

    def get(self, scope_key: str) -> ScopeRunState | None:
        raise NotImplementedError

    def set(self, state: ScopeRunState) -> None:
        raise NotImplementedError

    def list_all(self) -> list[ScopeRunState]:
        raise NotImplementedError


class InMemoryScopeGuardStorage(ScopeGuardStorage):
    def __init__(self) -> None:
        self._entries: dict[str, ScopeRunState] = {}
        self._lock = threading.RLock()

    def get(self, scope_key: str) -> ScopeRunState | None:
        with self._lock:
            state = self._entries.get(scope_key)
            if state is None:
                return None
            return ScopeRunState.from_dict(state.to_dict())

    def set(self, state: ScopeRunState) -> None:
        with self._lock:
            state.updated_at = time.time()
            self._entries[state.scope_key] = ScopeRunState.from_dict(state.to_dict())

    def list_all(self) -> list[ScopeRunState]:
        with self._lock:
            return [ScopeRunState.from_dict(s.to_dict()) for s in self._entries.values()]


class FileScopeGuardStorage(ScopeGuardStorage):
    def __init__(self, path: str | Path) -> None:
        self._file = LockedJsonDictFile(path)
        self._lock = threading.Lock()

    def get(self, scope_key: str) -> ScopeRunState | None:
        def read(data: dict[str, dict[str, Any]]) -> ScopeRunState | None:
            raw = data.get(scope_key)
            if raw is None:
                return None
            return ScopeRunState.from_dict(raw)

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def set(self, state: ScopeRunState) -> None:
        def mutate(data: dict[str, dict[str, Any]]) -> None:
            state.updated_at = time.time()
            data[state.scope_key] = state.to_dict()

        with self._lock:
            self._file.read_modify_write(mutate)

    def list_all(self) -> list[ScopeRunState]:
        def read(data: dict[str, dict[str, Any]]) -> list[ScopeRunState]:
            return [ScopeRunState.from_dict(raw) for raw in data.values()]

        with self._lock:
            return self._file.read_modify_write_no_save(read)


class ScopeGuard:
    """Run-scoped tool allowlist freeze (AF-008)."""

    def __init__(
        self,
        storage: ScopeGuardStorage | None = None,
        *,
        default_grant: ScopeGrant | None = None,
        on_violation: str = ON_VIOLATION_SOFT,
        exclude: list[str] | None = None,
        auto_bind: bool = True,
        missing_run_id_policy: str = MISSING_RUN_ID_POLICY_WARN,
    ) -> None:
        if on_violation not in ON_VIOLATION_MODES:
            raise ValueError(
                f"on_violation must be one of {sorted(ON_VIOLATION_MODES)}, "
                f"got {on_violation!r}"
            )
        if missing_run_id_policy not in MISSING_RUN_ID_POLICIES:
            raise ValueError(
                f"missing_run_id_policy must be one of "
                f"{sorted(MISSING_RUN_ID_POLICIES)}, got {missing_run_id_policy!r}"
            )
        self._storage = storage or InMemoryScopeGuardStorage()
        self._default_grant = default_grant
        self._on_violation = on_violation
        self._exclude = frozenset(exclude or [])
        self._auto_bind = auto_bind
        self._missing_run_id_policy = missing_run_id_policy

    @property
    def storage(self) -> ScopeGuardStorage:
        return self._storage

    @property
    def missing_run_id_policy(self) -> str:
        return self._missing_run_id_policy

    @property
    def default_grant(self) -> ScopeGrant | None:
        return self._default_grant

    def get_state(self, scope_key: str) -> ScopeRunState | None:
        return self._storage.get(scope_key)

    def bind(
        self,
        scope_key: str | None = None,
        grant: ScopeGrant | None = None,
        *,
        kwargs: dict[str, Any] | None = None,
    ) -> ScopeRunState:
        """Freeze an allowlist for ``scope_key``. Refuses silent widen on re-bind."""
        key = scope_key or resolve_loop_scope_key(kwargs=kwargs)
        if key is None:
            raise ValueError(
                "ScopeGuard.bind requires scope_key or run_id/thread_id in "
                "execution scope"
            )
        resolved = grant or self._default_grant
        if resolved is None:
            raise ValueError(
                "ScopeGuard.bind requires an explicit grant or a default_grant"
            )
        existing = self._storage.get(key)
        if existing is not None:
            if not resolved.is_at_most_as_permissive_as(existing.grant):
                raise ScopeWidenRefusedError(
                    f"ScopeGuard: refused to widen allowlist for run {key!r} "
                    f"(tools cannot expand mid-run)"
                )
            state = ScopeRunState(
                scope_key=key,
                grant=resolved,
                bound_at=existing.bound_at,
            )
            self._storage.set(state)
            return state
        state = ScopeRunState(scope_key=key, grant=resolved)
        self._storage.set(state)
        return state

    def check(
        self,
        tool: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Re-validate ``tool`` against the frozen allowlist before the body."""
        global _SCOPE_MISSING_WARNED
        del args  # allowlist check is name-only

        kwargs = dict(kwargs or {})
        if tool in self._exclude:
            return

        scope_key = enforce_run_identity(
            "ScopeGuard",
            tool=tool,
            policy=self._missing_run_id_policy,
            kwargs=kwargs,
        )
        if scope_key is None:
            return

        state = self._storage.get(scope_key)
        if state is None:
            if not self._auto_bind or self._default_grant is None:
                if not _SCOPE_MISSING_WARNED:
                    warnings.warn(
                        "ScopeGuard skipped: no frozen grant for this run; "
                        "call ScopeGuard.bind() or configure YAML scope_guard.",
                        stacklevel=2,
                    )
                    _SCOPE_MISSING_WARNED = True
                return
            state = self.bind(scope_key, self._default_grant)

        if tool not in state.grant.allowed_tools:
            allowed = sorted(state.grant.allowed_tools)
            llm_message = (
                f"Tool {tool!r} is outside the frozen allowlist for this run. "
                f"Allowed: {allowed}. The tool body was not executed."
            )
            if self._on_violation == ON_VIOLATION_HARD:
                raise LedgerHardBlockError(
                    f"ScopeGuard: run hard-blocked — {tool}: {VIOLATION_TOOL}. "
                    f"{llm_message}"
                )
            raise ToolBoundaryError(
                f"{tool}: {VIOLATION_TOOL}",
                violation=VIOLATION_TOOL,
                tool_name=tool,
                llm_message=llm_message,
                expected=f"one of {allowed}",
                actual=tool,
                recovery_hint=(
                    "Stay within the frozen allowlist for this run, or start a "
                    "new run_id with a wider grant."
                ),
            )


def _mark_scope_guarded(func: Callable[..., Any]) -> None:
    func._mycelium_scope_guarded = True  # type: ignore[attr-defined]


def scope_guard(
    guard: ScopeGuard,
    *,
    tool_name: str | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Async decorator: run ScopeGuard.check before the tool body."""

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        name = tool_name or func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            guard.check(name, args, dict(kwargs))
            return await func(*args, **kwargs)

        _mark_scope_guarded(wrapper)
        return wrapper

    return decorator


def scope_guard_sync(
    guard: ScopeGuard,
    *,
    tool_name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Sync decorator: run ScopeGuard.check before the tool body."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        name = tool_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            guard.check(name, args, dict(kwargs))
            return func(*args, **kwargs)

        _mark_scope_guarded(wrapper)
        return wrapper

    return decorator


def apply_scope_guard(
    func: Callable[..., Any],
    guard: ScopeGuard,
    *,
    tool_name: str | None = None,
) -> Callable[..., Any]:
    """Apply sync or async scope_guard wrapper based on ``func``."""
    name = tool_name or getattr(func, "__name__", "tool")
    if inspect.iscoroutinefunction(func):
        return scope_guard(guard, tool_name=name)(func)
    return scope_guard_sync(guard, tool_name=name)(func)


__all__ = [
    "ON_VIOLATION_HARD",
    "ON_VIOLATION_MODES",
    "ON_VIOLATION_SOFT",
    "VIOLATION_TOOL",
    "FileScopeGuardStorage",
    "InMemoryScopeGuardStorage",
    "ScopeGrant",
    "ScopeGuard",
    "ScopeGuardStorage",
    "ScopeRunState",
    "ScopeWidenRefusedError",
    "apply_scope_guard",
    "scope_guard",
    "scope_guard_sync",
]
