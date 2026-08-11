"""LoopGuard (AF-003): halt repeated identical actions across distinct dispatches.

Detects when an agent mints new ``tool_call_id``s for the same tool + args
and soft-blocks (``ToolBoundaryError``) then hard-blocks the whole run
(``LedgerHardBlockError``) until an operator releases it.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import threading
import time
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from mycelium.action_ledger import (
    LedgerAlreadyResolvedError,
    LedgerHardBlockError,
    LedgerReleaseRefusedError,
)
from mycelium.outcome_emit import (
    EVENT_RELEASE,
    EVENT_RESOLUTION,
    GATE_HARD_BLOCK,
    GATE_RELEASE,
    GATE_SOFT_BLOCK,
    OutcomeEmitter,
)
from mycelium.storage.json_file import LockedJsonDictFile
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import (
    SideEffectClass,
    args_fingerprint,
    derive_dispatch_id,
    get_active_execution_scope,
)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

UNCLASSIFIED_POLICY_WARN = "warn"
UNCLASSIFIED_POLICY_STRICT = "strict"

MISSING_RUN_ID_POLICY_WARN = "warn"
MISSING_RUN_ID_POLICY_ERROR = "error"
MISSING_RUN_ID_POLICIES = frozenset(
    {MISSING_RUN_ID_POLICY_WARN, MISSING_RUN_ID_POLICY_ERROR}
)

VERIFIED_CLEAR = "clear"
VERIFIED_ALLOW_ONCE = "allow-once"
VERIFIED_ABORT_RUN = "abort-run"
VERIFIED_RESOLUTIONS = frozenset(
    {VERIFIED_CLEAR, VERIFIED_ALLOW_ONCE, VERIFIED_ABORT_RUN}
)

DEFAULT_CONSECUTIVE_SOFT: dict[str, int] = {
    SideEffectClass.READ.value: 5,
    SideEffectClass.IDEMPOTENT_MUTATE.value: 3,
    SideEffectClass.KEYED_MUTATE.value: 2,
    SideEffectClass.NON_IDEMPOTENT_MUTATE.value: 2,
    SideEffectClass.IRREVERSIBLE.value: 2,
}

_SCOPE_MISSING_WARNED = False
_MISSING_IDENTITY_WARNED = False
_UNCLASSIFIED_WARNED_TOOLS: set[str] = set()


class MissingRunIdentityError(Exception):
    """Raised when an enabled run-scoped guard has no stable ``run_id``.

    ``run_id`` groups steps in one logical agent run. It is not a
    ``tool_call_id``, transition ``request_id``, provider idempotency key, or
    ``thread_id`` (a thread may span multiple runs).
    """

    def __init__(self, *, guard: str, tool: str | None = None) -> None:
        self.guard = guard
        self.tool = tool
        tool_bit = f" for tool {tool!r}" if tool else ""
        super().__init__(
            f"{guard} requires a stable run_id but none was available{tool_bit}.\n"
            "Supply TransitionScope(run_id=...) or configure the framework adapter.\n"
            "The protected tool was not executed."
        )


def action_hash(tool: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """Stable hash of tool name + canonical args (no dispatch identity)."""
    payload = f"{tool}:{args_fingerprint(args, kwargs)}"
    return hashlib.sha256(payload.encode()).hexdigest()


def parse_run_identity(value: Any) -> str | None:
    """Return a non-empty run/thread identity, or ``None`` if missing."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped if stripped else None


def resolve_run_id(*, kwargs: dict[str, Any] | None = None) -> str | None:
    """Return a valid host/framework ``run_id``, or ``None`` if missing."""
    kwargs = kwargs or {}
    scope = get_active_execution_scope()
    return parse_run_identity(kwargs.get("run_id")) or parse_run_identity(
        scope.run_id if scope else None
    )


def resolve_loop_scope_key(
    *,
    kwargs: dict[str, Any] | None = None,
) -> str | None:
    """Return ``run_id`` or fallback ``thread_id``; ``None`` if neither is set.

    Grouping still accepts ``thread_id`` (documented contract). Empty and
    whitespace-only values are not identities. This helper never invents a
    random id.
    """
    kwargs = kwargs or {}
    run_id = resolve_run_id(kwargs=kwargs)
    if run_id:
        return run_id
    scope = get_active_execution_scope()
    return parse_run_identity(kwargs.get("thread_id")) or parse_run_identity(
        scope.thread_id if scope else None
    )


def enforce_run_identity(
    guard_name: str,
    *,
    tool: str | None = None,
    policy: str = MISSING_RUN_ID_POLICY_WARN,
    kwargs: dict[str, Any] | None = None,
) -> str | None:
    """Resolve the grouping key, or warn/raise when identity is missing.

    ``error`` requires a valid ``run_id`` (``thread_id`` alone is not enough).
    ``warn`` preserves the skip-when-no-key behavior and uses ``thread_id`` as
    a grouping fallback when that documented contract applies.
    """
    if policy not in MISSING_RUN_ID_POLICIES:
        raise ValueError(
            f"missing_run_id_policy must be {MISSING_RUN_ID_POLICY_WARN!r} or "
            f"{MISSING_RUN_ID_POLICY_ERROR!r}, got {policy!r}"
        )
    kwargs = kwargs or {}
    if policy == MISSING_RUN_ID_POLICY_ERROR and resolve_run_id(kwargs=kwargs) is None:
        raise MissingRunIdentityError(guard=guard_name, tool=tool)
    scope_key = resolve_loop_scope_key(kwargs=kwargs)
    if scope_key is None:
        _warn_missing_run_identity(guard_name)
        return None
    return scope_key


def _warn_missing_run_identity(guard_name: str) -> None:
    global _MISSING_IDENTITY_WARNED
    if _MISSING_IDENTITY_WARNED:
        return
    warnings.warn(
        f"{guard_name} skipped: no stable run_id or thread_id was available. "
        "Supply TransitionScope(run_id=...) or configure the framework adapter. "
        "Protection was not applied.",
        stacklevel=4,
    )
    _MISSING_IDENTITY_WARNED = True


def reset_missing_run_identity_warnings() -> None:
    """Clear the bounded missing-identity warning (tests)."""
    global _MISSING_IDENTITY_WARNED, _SCOPE_MISSING_WARNED
    _MISSING_IDENTITY_WARNED = False
    _SCOPE_MISSING_WARNED = False


@dataclass
class LoopRunState:
    """Durable per-run loop detector state."""

    scope_key: str
    last_hash: str | None = None
    streak: int = 0
    last_dispatch_id: str | None = None
    soft_issued: dict[str, bool] = field(default_factory=dict)
    hard_blocked: bool = False
    blocked_action_hash: str | None = None
    allow_once_hash: str | None = None
    operator_resolution: str | None = None
    resolved_by: str | None = None
    reason: str | None = None
    resolved_at: float | None = None
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope_key": self.scope_key,
            "last_hash": self.last_hash,
            "streak": self.streak,
            "last_dispatch_id": self.last_dispatch_id,
            "soft_issued": dict(self.soft_issued),
            "hard_blocked": self.hard_blocked,
            "blocked_action_hash": self.blocked_action_hash,
            "allow_once_hash": self.allow_once_hash,
            "operator_resolution": self.operator_resolution,
            "resolved_by": self.resolved_by,
            "reason": self.reason,
            "resolved_at": self.resolved_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LoopRunState:
        soft_raw = data.get("soft_issued") or {}
        soft_issued = {str(k): bool(v) for k, v in soft_raw.items()}
        return cls(
            scope_key=str(data["scope_key"]),
            last_hash=data.get("last_hash"),
            streak=int(data.get("streak", 0)),
            last_dispatch_id=data.get("last_dispatch_id"),
            soft_issued=soft_issued,
            hard_blocked=bool(data.get("hard_blocked", False)),
            blocked_action_hash=data.get("blocked_action_hash"),
            allow_once_hash=data.get("allow_once_hash"),
            operator_resolution=data.get("operator_resolution"),
            resolved_by=data.get("resolved_by"),
            reason=data.get("reason"),
            resolved_at=data.get("resolved_at"),
            updated_at=float(data.get("updated_at") or time.time()),
        )


class LoopGuardStorage:
    """Storage protocol for per-run loop state."""

    def get(self, scope_key: str) -> LoopRunState | None:
        raise NotImplementedError

    def set(self, state: LoopRunState) -> None:
        raise NotImplementedError

    def update(
        self,
        scope_key: str,
        fn: Callable[[LoopRunState], T],
    ) -> T:
        """Atomically load-or-create state, apply ``fn``, persist.

        Closes the check get→set race: streak / soft / hard decisions must
        observe and write under one lock.
        """
        raise NotImplementedError

    def list_all(self) -> list[LoopRunState]:
        raise NotImplementedError


class InMemoryLoopGuardStorage(LoopGuardStorage):
    def __init__(self) -> None:
        self._entries: dict[str, LoopRunState] = {}
        self._lock = threading.RLock()

    def get(self, scope_key: str) -> LoopRunState | None:
        with self._lock:
            state = self._entries.get(scope_key)
            if state is None:
                return None
            return LoopRunState.from_dict(state.to_dict())

    def set(self, state: LoopRunState) -> None:
        with self._lock:
            state.updated_at = time.time()
            self._entries[state.scope_key] = LoopRunState.from_dict(state.to_dict())

    def update(
        self,
        scope_key: str,
        fn: Callable[[LoopRunState], T],
    ) -> T:
        with self._lock:
            existing = self._entries.get(scope_key)
            state = (
                LoopRunState.from_dict(existing.to_dict())
                if existing is not None
                else LoopRunState(scope_key=scope_key)
            )
            result = fn(state)
            state.updated_at = time.time()
            self._entries[scope_key] = LoopRunState.from_dict(state.to_dict())
            return result

    def list_all(self) -> list[LoopRunState]:
        with self._lock:
            return [LoopRunState.from_dict(s.to_dict()) for s in self._entries.values()]


class FileLoopGuardStorage(LoopGuardStorage):
    def __init__(self, path: str | Path) -> None:
        self._file = LockedJsonDictFile(path)
        self._lock = threading.Lock()

    def get(self, scope_key: str) -> LoopRunState | None:
        def read(data: dict[str, dict[str, Any]]) -> LoopRunState | None:
            raw = data.get(scope_key)
            if raw is None:
                return None
            return LoopRunState.from_dict(raw)

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def set(self, state: LoopRunState) -> None:
        def mutate(data: dict[str, dict[str, Any]]) -> None:
            state.updated_at = time.time()
            data[state.scope_key] = state.to_dict()

        with self._lock:
            self._file.read_modify_write(mutate)

    def update(
        self,
        scope_key: str,
        fn: Callable[[LoopRunState], T],
    ) -> T:
        def mutate(data: dict[str, dict[str, Any]]) -> T:
            raw = data.get(scope_key)
            state = (
                LoopRunState.from_dict(raw)
                if raw is not None
                else LoopRunState(scope_key=scope_key)
            )
            result = fn(state)
            state.updated_at = time.time()
            data[scope_key] = state.to_dict()
            return result

        with self._lock:
            return self._file.read_modify_write(mutate)

    def list_all(self) -> list[LoopRunState]:
        def read(data: dict[str, dict[str, Any]]) -> list[LoopRunState]:
            return [LoopRunState.from_dict(raw) for raw in data.values()]

        with self._lock:
            return self._file.read_modify_write_no_save(read)


class LoopGuard:
    """Run-scoped consecutive action-hash detector (AF-003)."""

    def __init__(
        self,
        storage: LoopGuardStorage | None = None,
        *,
        consecutive_soft: dict[str, int] | None = None,
        escalate_after_soft: int = 1,
        unclassified_policy: str = UNCLASSIFIED_POLICY_WARN,
        exclude: list[str] | None = None,
        outcome_emitter: OutcomeEmitter | None = None,
        agent_id: str = "loop-guard",
        missing_run_id_policy: str = MISSING_RUN_ID_POLICY_WARN,
    ) -> None:
        if unclassified_policy not in (
            UNCLASSIFIED_POLICY_WARN,
            UNCLASSIFIED_POLICY_STRICT,
        ):
            raise ValueError(
                f"unclassified_policy must be {UNCLASSIFIED_POLICY_WARN!r} or "
                f"{UNCLASSIFIED_POLICY_STRICT!r}, got {unclassified_policy!r}"
            )
        if escalate_after_soft < 1:
            raise ValueError("escalate_after_soft must be >= 1")
        if missing_run_id_policy not in MISSING_RUN_ID_POLICIES:
            raise ValueError(
                f"missing_run_id_policy must be {MISSING_RUN_ID_POLICY_WARN!r} or "
                f"{MISSING_RUN_ID_POLICY_ERROR!r}, got {missing_run_id_policy!r}"
            )
        self._storage = storage or InMemoryLoopGuardStorage()
        merged = dict(DEFAULT_CONSECUTIVE_SOFT)
        if consecutive_soft:
            for key, value in consecutive_soft.items():
                merged[str(key)] = int(value)
        self._consecutive_soft = merged
        self._escalate_after_soft = escalate_after_soft
        self._unclassified_policy = unclassified_policy
        self._exclude = frozenset(exclude or [])
        self._outcome_emitter = outcome_emitter
        self._agent_id = agent_id
        self._missing_run_id_policy = missing_run_id_policy

    @property
    def storage(self) -> LoopGuardStorage:
        return self._storage

    @property
    def missing_run_id_policy(self) -> str:
        return self._missing_run_id_policy

    def threshold_for(self, side_effect_class: SideEffectClass | None) -> int:
        if side_effect_class is None:
            if self._unclassified_policy == UNCLASSIFIED_POLICY_STRICT:
                return self._consecutive_soft[SideEffectClass.NON_IDEMPOTENT_MUTATE.value]
            return self._consecutive_soft[SideEffectClass.READ.value]
        return self._consecutive_soft.get(
            side_effect_class.value,
            self._consecutive_soft[SideEffectClass.READ.value],
        )

    def get_state(self, scope_key: str) -> LoopRunState | None:
        return self._storage.get(scope_key)

    def check(
        self,
        tool: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        side_effect_class: SideEffectClass | None = None,
        consecutive_soft_override: int | None = None,
    ) -> None:
        """Record the dispatch and raise soft/hard when thresholds trip.

        Call **before** the tool body (and before ledger claim). On success the
        call is allowed through; on soft/hard the body must not run.
        """
        kwargs = dict(kwargs or {})
        if tool in self._exclude:
            return

        scope_key = enforce_run_identity(
            "LoopGuard",
            tool=tool,
            policy=self._missing_run_id_policy,
            kwargs=kwargs,
        )
        if scope_key is None:
            return

        if side_effect_class is None and tool not in _UNCLASSIFIED_WARNED_TOOLS:
            n_label = (
                "mutate N=2"
                if self._unclassified_policy == UNCLASSIFIED_POLICY_STRICT
                else "read N=5"
            )
            warnings.warn(
                f"LoopGuard tool {tool!r} has no side_effect_class; "
                f"using {n_label} "
                f"(unclassified_policy={self._unclassified_policy!r}).",
                stacklevel=2,
            )
            _UNCLASSIFIED_WARNED_TOOLS.add(tool)

        threshold = (
            consecutive_soft_override
            if consecutive_soft_override is not None
            else self.threshold_for(side_effect_class)
        )
        dispatch_id = derive_dispatch_id(kwargs)
        ahash = action_hash(tool, args, kwargs)

        # Decision + persist under one storage lock (closes get→set race).
        outcome: dict[str, Any] = {"gate": None, "exc": None}

        def apply(state: LoopRunState) -> None:
            if state.hard_blocked:
                outcome["gate"] = GATE_HARD_BLOCK
                outcome["exc"] = LedgerHardBlockError(
                    f"LoopGuard: run {scope_key!r} is hard-blocked after repeated "
                    f"action {tool!r}. Release with: mycelium loops release {scope_key} "
                    f"--verified clear|allow-once|abort-run --by … --reason …"
                )
                return

            if state.allow_once_hash is not None and state.allow_once_hash == ahash:
                state.allow_once_hash = None
                state.last_hash = ahash
                state.streak = 1
                state.last_dispatch_id = dispatch_id
                state.soft_issued.pop(ahash, None)
                return

            if (
                dispatch_id is not None
                and state.last_dispatch_id is not None
                and dispatch_id == state.last_dispatch_id
            ):
                return

            if state.last_hash == ahash:
                streak = state.streak + 1
            else:
                streak = 1

            if state.soft_issued.get(ahash):
                state.hard_blocked = True
                state.blocked_action_hash = ahash
                state.last_hash = ahash
                state.streak = streak
                state.last_dispatch_id = dispatch_id
                state.operator_resolution = None
                state.resolved_by = None
                state.reason = None
                state.resolved_at = None
                outcome["gate"] = GATE_HARD_BLOCK
                outcome["exc"] = LedgerHardBlockError(
                    f"LoopGuard: run {scope_key!r} hard-blocked — {tool!r} repeated "
                    f"after soft warning. Release with: mycelium loops release "
                    f"{scope_key} --verified clear|allow-once|abort-run --by … "
                    f"--reason …"
                )
                return

            if streak >= threshold:
                state.soft_issued[ahash] = True
                state.last_hash = ahash
                state.streak = streak
                state.last_dispatch_id = dispatch_id
                state.blocked_action_hash = ahash
                outcome["gate"] = GATE_SOFT_BLOCK
                outcome["exc"] = ToolBoundaryError(
                    f"{tool}: loop detected",
                    violation="loop_detected",
                    tool_name=tool,
                    llm_message=(
                        f"Loop detected: {tool!r} with the same arguments repeated "
                        f"{streak} times (threshold {threshold}). Change strategy "
                        "or stop. The tool body was not executed."
                    ),
                )
                return

            state.last_hash = ahash
            state.streak = streak
            state.last_dispatch_id = dispatch_id

        self._storage.update(scope_key, apply)
        exc = outcome["exc"]
        if exc is None:
            return
        gate = outcome["gate"]
        self._emit(
            tool=tool,
            scope_key=scope_key,
            event=EVENT_RESOLUTION,
            gate=gate,
            side_effect_class=side_effect_class,
            error_class=type(exc).__name__,
        )
        raise exc

    def release(
        self,
        scope_key: str,
        *,
        verified: str,
        by: str,
        reason: str,
        action_hash_for_allow_once: str | None = None,
    ) -> LoopRunState:
        """Operator release for a hard-blocked (or abortable) run."""
        if verified not in VERIFIED_RESOLUTIONS:
            raise LedgerReleaseRefusedError(
                f"unknown --verified {verified!r}; "
                f"expected one of {sorted(VERIFIED_RESOLUTIONS)}"
            )
        state = self._storage.get(scope_key)
        if state is None:
            raise LedgerReleaseRefusedError(
                f"no loop-guard state for run {scope_key!r}"
            )
        if state.operator_resolution is not None:
            raise LedgerAlreadyResolvedError(
                f"run {scope_key!r} already released "
                f"({state.operator_resolution!r} by {state.resolved_by!r})"
            )
        if not state.hard_blocked and verified != VERIFIED_ABORT_RUN:
            raise LedgerReleaseRefusedError(
                f"run {scope_key!r} is not hard-blocked; nothing to release"
            )

        now = time.time()
        state.operator_resolution = verified
        state.resolved_by = by
        state.reason = reason
        state.resolved_at = now

        if verified == VERIFIED_CLEAR:
            state.hard_blocked = False
            state.allow_once_hash = None
            state.last_hash = None
            state.streak = 0
            state.last_dispatch_id = None
            state.soft_issued = {}
            state.blocked_action_hash = None
        elif verified == VERIFIED_ALLOW_ONCE:
            once_hash = action_hash_for_allow_once or state.blocked_action_hash
            if once_hash is None:
                raise LedgerReleaseRefusedError(
                    f"run {scope_key!r} has no blocked action hash for allow-once"
                )
            state.hard_blocked = False
            state.allow_once_hash = once_hash
            state.soft_issued.pop(once_hash, None)
        else:  # abort-run
            state.hard_blocked = True
            state.allow_once_hash = None

        self._storage.set(state)
        self._emit(
            tool="*",
            scope_key=scope_key,
            event=EVENT_RELEASE,
            gate=GATE_RELEASE,
            error_class=None,
        )
        return state

    def _emit(
        self,
        *,
        tool: str,
        scope_key: str,
        event: str,
        gate: str | None,
        side_effect_class: SideEffectClass | None = None,
        error_class: str | None,
    ) -> None:
        if self._outcome_emitter is None:
            return
        try:
            self._outcome_emitter.emit_event(
                tool=tool,
                request_id=f"loop:{scope_key}",
                event=event,
                gate=gate,
                side_effect_class=(
                    side_effect_class.value if side_effect_class is not None else None
                ),
                error_class=error_class,
            )
        except Exception:  # noqa: BLE001 — telemetry must never break the tool path
            pass


def _mark_loop_guarded(func: Callable[..., Any]) -> None:
    func._mycelium_loop_guarded = True  # type: ignore[attr-defined]


def loop_guard(
    guard: LoopGuard,
    *,
    tool_name: str | None = None,
    side_effect_class: SideEffectClass | None = None,
    consecutive_soft: int | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Async decorator: run LoopGuard.check before the tool body."""

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        name = tool_name or func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            guard.check(
                name,
                args,
                dict(kwargs),
                side_effect_class=side_effect_class,
                consecutive_soft_override=consecutive_soft,
            )
            return await func(*args, **kwargs)

        _mark_loop_guarded(wrapper)
        return wrapper

    return decorator


def loop_guard_sync(
    guard: LoopGuard,
    *,
    tool_name: str | None = None,
    side_effect_class: SideEffectClass | None = None,
    consecutive_soft: int | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Sync decorator: run LoopGuard.check before the tool body."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        name = tool_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            guard.check(
                name,
                args,
                dict(kwargs),
                side_effect_class=side_effect_class,
                consecutive_soft_override=consecutive_soft,
            )
            return func(*args, **kwargs)

        _mark_loop_guarded(wrapper)
        return wrapper

    return decorator


def apply_loop_guard(
    func: Callable[..., Any],
    guard: LoopGuard,
    *,
    tool_name: str | None = None,
    side_effect_class: SideEffectClass | None = None,
    consecutive_soft: int | None = None,
) -> Callable[..., Any]:
    """Apply sync or async loop_guard wrapper based on ``func``."""
    name = tool_name or getattr(func, "__name__", "tool")
    if inspect.iscoroutinefunction(func):
        return loop_guard(
            guard,
            tool_name=name,
            side_effect_class=side_effect_class,
            consecutive_soft=consecutive_soft,
        )(func)
    return loop_guard_sync(
        guard,
        tool_name=name,
        side_effect_class=side_effect_class,
        consecutive_soft=consecutive_soft,
    )(func)


__all__ = [
    "DEFAULT_CONSECUTIVE_SOFT",
    "MISSING_RUN_ID_POLICIES",
    "MISSING_RUN_ID_POLICY_ERROR",
    "MISSING_RUN_ID_POLICY_WARN",
    "UNCLASSIFIED_POLICY_STRICT",
    "UNCLASSIFIED_POLICY_WARN",
    "VERIFIED_ABORT_RUN",
    "VERIFIED_ALLOW_ONCE",
    "VERIFIED_CLEAR",
    "VERIFIED_RESOLUTIONS",
    "FileLoopGuardStorage",
    "InMemoryLoopGuardStorage",
    "LoopGuard",
    "LoopGuardStorage",
    "LoopRunState",
    "MissingRunIdentityError",
    "action_hash",
    "apply_loop_guard",
    "enforce_run_identity",
    "loop_guard",
    "loop_guard_sync",
    "parse_run_identity",
    "resolve_loop_scope_key",
    "resolve_run_id",
]
