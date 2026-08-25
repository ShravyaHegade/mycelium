"""CompletionContract (AF-007): refuse terminal output while required subtasks are pending.

Host-declared checklist (YAML / code). Subtask ids are marked
``success`` / ``failed`` / ``abandoned`` (reason required for abandoned).
Unmarked **required** → refuse terminal; unmarked **optional** → warn and allow.

Public vocabulary: allow / allow_with_warnings / refuse (not soft/hard).
"""

from __future__ import annotations

import functools
import inspect
import threading
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypeVar

from mycelium.loop_guard import resolve_loop_scope_key
from mycelium.storage.atomic_state import AtomicStateBackend, NamespacedAtomicStorage
from mycelium.storage.json_file import LockedJsonDictFile

T = TypeVar("T")

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_ABANDONED = "abandoned"
STATUS_PENDING = "pending"
RESOLVED_STATUSES = frozenset({STATUS_SUCCESS, STATUS_FAILED, STATUS_ABANDONED})

TerminalVerdict = Literal["allow", "allow_with_warnings", "refuse"]

_SCOPE_MISSING_WARNED = False

COMPLETION_WRAPPED_MARK = "_mycelium_completion_wrapped"
TERMINAL_ADAPTER_LANGGRAPH = "langgraph"
TERMINAL_ADAPTER_FINAL_MESSAGE = "final_message"
TERMINAL_ADAPTER_GRAPH_END = "graph_end"
TERMINAL_ADAPTER_MANUAL = "manual"

_MYCELIUM_WRAPPER_MARKERS = (
    COMPLETION_WRAPPED_MARK,
    "_mycelium_config_applied",
    "_mycelium_ledger",
    "_mycelium_ledger_instance",
    "_mycelium_task_ledger",
    "_mycelium_bounded",
    "_mycelium_protected",
    "_mycelium_loop_guarded",
    "_mycelium_budget_guarded",
    "_mycelium_scope_guarded",
    "_mycelium_state_authority",
    "_mycelium_langgraph_integration",
)

_active_completion_contract: CompletionContract | None = None
_registered_terminal_adapters: set[str] = set()
_unwired_completion_warned = False


def get_active_completion_contract() -> CompletionContract | None:
    """Return the contract activated by the latest ``completion:`` config."""
    return _active_completion_contract


def set_active_completion_contract(contract: CompletionContract | None) -> None:
    """Bind or clear the process-wide contract used by automatic terminals."""
    global _active_completion_contract
    _active_completion_contract = contract


def registered_terminal_adapters() -> frozenset[str]:
    """Return adapters declared via ``register_terminal_adapter``."""
    return frozenset(_registered_terminal_adapters)


def register_terminal_adapter(name: str) -> None:
    """Declare a custom-runtime terminal adapter before ``load_config``.

    Supported frameworks (LangGraph END) register themselves. Custom
    runtimes call this, then ``wrap_final_message`` / ``gate_graph_end`` /
    ``complete_run``. Production startup requires at least one verified
    adapter when ``completion:`` is enabled.
    """
    label = str(name).strip()
    if not label:
        raise ValueError("terminal adapter name must be non-empty")
    _registered_terminal_adapters.add(label)


def reset_completion_terminal_state() -> None:
    """Clear process-wide completion terminal state (tests)."""
    global _active_completion_contract, _unwired_completion_warned
    _active_completion_contract = None
    _registered_terminal_adapters.clear()
    _unwired_completion_warned = False


def _preserve_mycelium_markers(src: Any, dst: Any) -> None:
    for mark in _MYCELIUM_WRAPPER_MARKERS:
        if hasattr(src, mark):
            setattr(dst, mark, getattr(src, mark))
    signature = getattr(src, "__signature__", None)
    if signature is not None:
        dst.__signature__ = signature


class CompletionError(Exception):
    """Base error for completion-contract operations."""


class CompletionMarkError(CompletionError):
    """Invalid mark (unknown id, bad status, missing abandon reason)."""


class CompletionRefusedError(CompletionError):
    """Terminal refused: one or more required subtasks are still pending."""

    def __init__(
        self,
        message: str,
        *,
        scope_key: str,
        pending_required: list[str],
    ) -> None:
        super().__init__(message)
        self.scope_key = scope_key
        self.pending_required = list(pending_required)


@dataclass
class SubtaskMark:
    """One subtask resolution mark."""

    status: str
    reason: str | None = None
    marked_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "marked_at": self.marked_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SubtaskMark:
        return cls(
            status=str(data["status"]),
            reason=data.get("reason"),
            marked_at=float(data.get("marked_at") or time.time()),
        )


@dataclass
class CompletionRunState:
    """Durable per-run completion checklist state."""

    scope_key: str
    required: list[str] = field(default_factory=list)
    optional: list[str] = field(default_factory=list)
    marks: dict[str, SubtaskMark] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope_key": self.scope_key,
            "required": list(self.required),
            "optional": list(self.optional),
            "marks": {k: v.to_dict() for k, v in self.marks.items()},
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CompletionRunState:
        marks_raw = data.get("marks") or {}
        marks = {
            str(k): SubtaskMark.from_dict(v) if isinstance(v, dict) else SubtaskMark(status=str(v))
            for k, v in marks_raw.items()
        }
        return cls(
            scope_key=str(data["scope_key"]),
            required=[str(x) for x in (data.get("required") or [])],
            optional=[str(x) for x in (data.get("optional") or [])],
            marks=marks,
            updated_at=float(data.get("updated_at") or time.time()),
        )

    def known_ids(self) -> set[str]:
        return set(self.required) | set(self.optional)

    def pending_required(self) -> list[str]:
        return [
            sid
            for sid in self.required
            if sid not in self.marks
            or self.marks[sid].status not in RESOLVED_STATUSES
        ]

    def pending_optional(self) -> list[str]:
        return [
            sid
            for sid in self.optional
            if sid not in self.marks
            or self.marks[sid].status not in RESOLVED_STATUSES
        ]


class CompletionStorage:
    """Storage protocol for per-run completion state."""

    def get(self, scope_key: str) -> CompletionRunState | None:
        raise NotImplementedError

    def set(self, state: CompletionRunState) -> None:
        raise NotImplementedError

    def update(
        self,
        scope_key: str,
        fn: Callable[[CompletionRunState | None], tuple[CompletionRunState, T]],
    ) -> T:
        raise NotImplementedError

    def list_all(self) -> list[CompletionRunState]:
        raise NotImplementedError


class InMemoryCompletionStorage(CompletionStorage):
    def __init__(self) -> None:
        self._entries: dict[str, CompletionRunState] = {}
        self._lock = threading.RLock()

    def get(self, scope_key: str) -> CompletionRunState | None:
        with self._lock:
            state = self._entries.get(scope_key)
            if state is None:
                return None
            return CompletionRunState.from_dict(state.to_dict())

    def set(self, state: CompletionRunState) -> None:
        with self._lock:
            state.updated_at = time.time()
            self._entries[state.scope_key] = CompletionRunState.from_dict(
                state.to_dict()
            )

    def update(
        self,
        scope_key: str,
        fn: Callable[[CompletionRunState | None], tuple[CompletionRunState, T]],
    ) -> T:
        with self._lock:
            raw = self._entries.get(scope_key)
            existing = None if raw is None else CompletionRunState.from_dict(raw.to_dict())
            state, result = fn(existing)
            state.updated_at = time.time()
            self._entries[scope_key] = CompletionRunState.from_dict(state.to_dict())
            return result

    def list_all(self) -> list[CompletionRunState]:
        with self._lock:
            return [
                CompletionRunState.from_dict(s.to_dict())
                for s in self._entries.values()
            ]


class FileCompletionStorage(CompletionStorage):
    def __init__(self, path: str | Path) -> None:
        self._file = LockedJsonDictFile(path)
        self._lock = threading.Lock()

    def get(self, scope_key: str) -> CompletionRunState | None:
        def read(data: dict[str, dict[str, Any]]) -> CompletionRunState | None:
            raw = data.get(scope_key)
            if raw is None:
                return None
            return CompletionRunState.from_dict(raw)

        with self._lock:
            return self._file.read_modify_write_no_save(read)

    def set(self, state: CompletionRunState) -> None:
        def mutate(data: dict[str, dict[str, Any]]) -> None:
            state.updated_at = time.time()
            data[state.scope_key] = state.to_dict()

        with self._lock:
            self._file.read_modify_write(mutate)

    def update(
        self,
        scope_key: str,
        fn: Callable[[CompletionRunState | None], tuple[CompletionRunState, T]],
    ) -> T:
        def mutate(data: dict[str, dict[str, Any]]) -> T:
            raw = data.get(scope_key)
            existing = None if raw is None else CompletionRunState.from_dict(raw)
            state, result = fn(existing)
            state.updated_at = time.time()
            data[scope_key] = state.to_dict()
            return result

        with self._lock:
            return self._file.read_modify_write(mutate)

    def list_all(self) -> list[CompletionRunState]:
        def read(data: dict[str, dict[str, Any]]) -> list[CompletionRunState]:
            return [CompletionRunState.from_dict(raw) for raw in data.values()]

        with self._lock:
            return self._file.read_modify_write_no_save(read)


class AtomicCompletionStorage(CompletionStorage):
    """Completion marks stored through the shared CAS backend."""

    def __init__(
        self,
        backend: AtomicStateBackend,
        *,
        namespace: str = "completion",
    ) -> None:
        self._storage = NamespacedAtomicStorage(
            backend,
            namespace,
            from_dict=CompletionRunState.from_dict,
            to_dict=lambda state: state.to_dict(),
        )

    def get(self, scope_key: str) -> CompletionRunState | None:
        return self._storage.get(scope_key)

    def set(self, state: CompletionRunState) -> None:
        state.updated_at = time.time()
        self._storage.set(state.scope_key, state)

    def update(
        self,
        scope_key: str,
        fn: Callable[[CompletionRunState | None], tuple[CompletionRunState, T]],
    ) -> T:
        def mutate(
            existing: CompletionRunState | None,
        ) -> tuple[CompletionRunState, T]:
            state, result = fn(existing)
            state.updated_at = time.time()
            return state, result

        return self._storage.update_optional(scope_key, mutate)

    def list_all(self) -> list[CompletionRunState]:
        return self._storage.list_all()


@dataclass(frozen=True)
class CompleteRunResult:
    """Outcome of ``complete_run`` when terminal is allowed."""

    verdict: TerminalVerdict
    scope_key: str
    pending_optional: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class CompletionContract:
    """Host-declared run checklist (AF-007)."""

    def __init__(
        self,
        storage: CompletionStorage | None = None,
        *,
        required: list[str] | None = None,
        optional: list[str] | None = None,
    ) -> None:
        req = [str(x) for x in (required or [])]
        opt = [str(x) for x in (optional or [])]
        overlap = set(req) & set(opt)
        if overlap:
            raise ValueError(
                f"subtask ids cannot be both required and optional: {sorted(overlap)}"
            )
        if not req and not opt:
            raise ValueError("completion contract needs at least one required or optional id")
        self._storage = storage or InMemoryCompletionStorage()
        self._required = req
        self._optional = opt

    @property
    def storage(self) -> CompletionStorage:
        return self._storage

    @property
    def required(self) -> list[str]:
        return list(self._required)

    @property
    def optional(self) -> list[str]:
        return list(self._optional)

    def _ensure_state(self, scope_key: str) -> CompletionRunState:
        current = self._storage.get(scope_key)
        if current is not None and (current.required or current.optional):
            return current

        def ensure(
            existing: CompletionRunState | None,
        ) -> tuple[CompletionRunState, CompletionRunState]:
            state = existing or CompletionRunState(scope_key=scope_key)
            if not state.required and not state.optional:
                state.required = list(self._required)
                state.optional = list(self._optional)
            return state, state

        return self._storage.update(scope_key, ensure)

    def get_state(self, scope_key: str) -> CompletionRunState | None:
        return self._storage.get(scope_key)

    def bind_run(self, scope_key: str) -> CompletionRunState:
        """Ensure run state exists with this contract's required/optional lists."""
        def bind(
            existing: CompletionRunState | None,
        ) -> tuple[CompletionRunState, CompletionRunState]:
            state = CompletionRunState(
                scope_key=scope_key,
                required=list(self._required),
                optional=list(self._optional),
                marks=dict(existing.marks) if existing is not None else {},
            )
            return state, state

        return self._storage.update(scope_key, bind)

    def mark(
        self,
        subtask_id: str,
        status: str,
        *,
        reason: str | None = None,
        scope_key: str | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> CompletionRunState:
        """Mark a subtask resolved for the active (or explicit) run."""
        key = scope_key or resolve_loop_scope_key(kwargs=kwargs)
        if key is None:
            raise CompletionMarkError(
                "CompletionContract.mark requires run_id or thread_id "
                "(execution_scope / TransitionScope)"
            )
        sid = str(subtask_id)
        status_n = str(status).strip().lower()
        if status_n not in RESOLVED_STATUSES:
            raise CompletionMarkError(
                f"status must be one of {sorted(RESOLVED_STATUSES)}, got {status!r}"
            )
        if status_n == STATUS_ABANDONED and not (reason and str(reason).strip()):
            raise CompletionMarkError(
                "abandoned marks require a non-empty reason"
            )
        def mark_atomic(
            existing: CompletionRunState | None,
        ) -> tuple[CompletionRunState, CompletionRunState]:
            state = existing or CompletionRunState(
                scope_key=key,
                required=list(self._required),
                optional=list(self._optional),
            )
            if not state.required and not state.optional:
                state.required = list(self._required)
                state.optional = list(self._optional)
            if sid not in state.known_ids():
                raise CompletionMarkError(
                    f"unknown subtask id {sid!r}; known: {sorted(state.known_ids())}"
                )
            state.marks[sid] = SubtaskMark(
                status=status_n,
                reason=str(reason).strip() if reason else None,
            )
            return state, state

        return self._storage.update(key, mark_atomic)

    def check_terminal(
        self,
        *,
        scope_key: str | None = None,
        kwargs: dict[str, Any] | None = None,
        raise_on_refuse: bool = True,
    ) -> CompleteRunResult | None:
        """Evaluate terminal readiness.

        Returns ``CompleteRunResult`` on allow / allow_with_warnings.
        On refuse: raises ``CompletionRefusedError`` if ``raise_on_refuse``,
        else returns a result with ``verdict='refuse'``.

        Missing scope: warn once and return ``None`` (skip gate).
        """
        global _SCOPE_MISSING_WARNED
        key = scope_key or resolve_loop_scope_key(kwargs=kwargs)
        if key is None:
            if not _SCOPE_MISSING_WARNED:
                warnings.warn(
                    "CompletionContract skipped: no run_id or thread_id in "
                    "execution scope; wire transition.scope_from / execution_scope "
                    "for AF-007 protection.",
                    UserWarning,
                    stacklevel=2,
                )
                _SCOPE_MISSING_WARNED = True
            return None

        state = self._ensure_state(key)
        pending_req = state.pending_required()
        pending_opt = state.pending_optional()

        if pending_req:
            msg = (
                f"CompletionContract: refuse terminal for run {key!r} — "
                f"required subtasks still pending: {pending_req}. "
                f"Mark each success|failed|abandoned (abandoned needs --reason), "
                f"then retry complete_run / END."
            )
            if raise_on_refuse:
                raise CompletionRefusedError(
                    msg, scope_key=key, pending_required=pending_req
                )
            return CompleteRunResult(
                verdict="refuse",
                scope_key=key,
                pending_optional=pending_opt,
                warnings=[msg],
            )

        warn_msgs: list[str] = []
        if pending_opt:
            warn_msgs.append(
                f"CompletionContract: optional subtasks still pending "
                f"(terminal allowed): {pending_opt}"
            )
            for w in warn_msgs:
                warnings.warn(w, UserWarning, stacklevel=2)

        verdict: TerminalVerdict = (
            "allow_with_warnings" if pending_opt else "allow"
        )
        return CompleteRunResult(
            verdict=verdict,
            scope_key=key,
            pending_optional=pending_opt,
            warnings=warn_msgs,
        )

    def complete_run(
        self,
        *,
        scope_key: str | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> CompleteRunResult | None:
        """Gate a terminal attempt (primitive entry point).

        Raises ``CompletionRefusedError`` when required items are pending.
        Returns ``None`` when scope is missing (skip).
        """
        return self.check_terminal(
            scope_key=scope_key, kwargs=kwargs, raise_on_refuse=True
        )


def wrap_final_message(
    contract: CompletionContract,
    emit: Callable[..., Any],
) -> Callable[..., Any]:
    """Adapter: call ``complete_run`` before emitting a final message / answer.

    Official LangGraph integrations are wired automatically from YAML.
    Use this for custom runtimes that own an explicit "emit final answer"
    callable. Applying twice is idempotent.
    """
    if getattr(emit, COMPLETION_WRAPPED_MARK, False):
        return emit

    if inspect.iscoroutinefunction(emit):

        @functools.wraps(emit)
        async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
            contract.complete_run(kwargs=kwargs)
            return await emit(*args, **kwargs)

        wrapped: Callable[..., Any] = async_wrapped
    else:

        @functools.wraps(emit)
        def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
            contract.complete_run(kwargs=kwargs)
            return emit(*args, **kwargs)

        wrapped = sync_wrapped

    _preserve_mycelium_markers(emit, wrapped)
    setattr(wrapped, COMPLETION_WRAPPED_MARK, True)
    return wrapped


def gate_graph_end(
    contract: CompletionContract,
    *,
    scope_key: str | None = None,
    kwargs: dict[str, Any] | None = None,
) -> CompleteRunResult | None:
    """Adapter: LangGraph (or other) END / last-node hook → ``complete_run``.

    Supported LangGraph graphs are patched automatically. Call this only
    from a custom runtime's genuine terminal boundary.
    """
    return contract.complete_run(scope_key=scope_key, kwargs=kwargs)
