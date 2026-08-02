"""StateAuthority: refuse tool execution derived from a superseded state ref.

Mycelium's ActionLedger answers "has this transition already been claimed?"
This module answers a different question: "was this call derived from state
that is still current?"

Wraps **outside** ``@ledger`` / ``@loop_guard`` so a stale checkpoint
redispatch with a *new* ``tool_call_id`` is blocked before any claim.
"""

from __future__ import annotations

import functools
import inspect
import warnings
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, Protocol, TypeVar

from mycelium.action_ledger import LedgerHardBlockError
from mycelium.outcome_emit import (
    EVENT_RESOLUTION,
    GATE_HARD_BLOCK,
    GATE_SOFT_BLOCK,
    OutcomeEmitter,
)
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import (
    SideEffectClass,
    get_active_execution_scope,
)

P = ParamSpec("P")
R = TypeVar("R")

ON_MISMATCH_SOFT = "soft"
ON_MISMATCH_HARD = "hard"
ON_MISMATCH_MODES = frozenset({ON_MISMATCH_SOFT, ON_MISMATCH_HARD})

VIOLATION_SUPERSEDED = "state_superseded"
VIOLATION_MISSING = "state_ref_missing"
VIOLATION_UNRESOLVED = "state_ref_unresolved"

_RESOLVER_NONE_WARNED = False


class CanonicalStateRefResolver(Protocol):
    """Host-supplied current canonical state / checkpoint identity."""

    def __call__(
        self,
        *,
        tool: str,
        thread_id: str | None,
        run_id: str | None,
        kwargs: dict[str, Any],
    ) -> str | None: ...


def extract_state_ref(kwargs: dict[str, Any] | None) -> str | None:
    """Return frozen ``state_ref`` from tool kwargs, if present."""
    if not kwargs:
        return None
    value = kwargs.get("state_ref")
    if value is None or value == "":
        return None
    return str(value)


def extract_decision_id(kwargs: dict[str, Any] | None) -> str | None:
    """Return optional ``decision_id`` from tool kwargs, if present."""
    if not kwargs:
        return None
    value = kwargs.get("decision_id")
    if value is None or value == "":
        return None
    return str(value)


def _scope_ids(kwargs: dict[str, Any]) -> tuple[str | None, str | None]:
    scope = get_active_execution_scope()
    thread_id = kwargs.get("thread_id") or (scope.thread_id if scope else None)
    run_id = kwargs.get("run_id") or (scope.run_id if scope else None)
    return (
        str(thread_id) if thread_id is not None else None,
        str(run_id) if run_id is not None else None,
    )


class StateAuthority:
    """Pre-claim execution gate: freeze-at-decide, compare-at-execute.

    The host freezes ``state_ref`` (checkpoint id / state version / content
    hash) when the decision is made and passes it on each tool call. At
    execute time this gate asks ``get_canonical_state_ref`` for the current
    canonical identity and blocks on mismatch — *before* ledger claim.
    """

    def __init__(
        self,
        get_canonical_state_ref: CanonicalStateRefResolver,
        *,
        require_state_ref: bool = False,
        on_mismatch: str = ON_MISMATCH_HARD,
        on_missing: str = ON_MISMATCH_HARD,
        exclude: list[str] | None = None,
        outcome_emitter: OutcomeEmitter | None = None,
        agent_id: str = "state-authority",
    ) -> None:
        if on_mismatch not in ON_MISMATCH_MODES:
            raise ValueError(
                f"on_mismatch must be one of {sorted(ON_MISMATCH_MODES)}, "
                f"got {on_mismatch!r}"
            )
        if on_missing not in ON_MISMATCH_MODES:
            raise ValueError(
                f"on_missing must be one of {sorted(ON_MISMATCH_MODES)}, "
                f"got {on_missing!r}"
            )
        self._get_canonical = get_canonical_state_ref
        self._require_state_ref = require_state_ref
        self._on_mismatch = on_mismatch
        self._on_missing = on_missing
        self._exclude = frozenset(exclude or [])
        self._outcome_emitter = outcome_emitter
        self._agent_id = agent_id

    @property
    def require_state_ref(self) -> bool:
        return self._require_state_ref

    def check(
        self,
        tool: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        side_effect_class: SideEffectClass | None = None,
    ) -> None:
        """Validate state authority before the tool body (and before claim).

        ``args`` is accepted for wrapper symmetry; identity comes from kwargs.
        """
        del args  # bookkeeping kwargs / resolver context only
        global _RESOLVER_NONE_WARNED

        kwargs = dict(kwargs or {})
        if tool in self._exclude:
            return

        frozen = extract_state_ref(kwargs)
        decision_id = extract_decision_id(kwargs)
        thread_id, run_id = _scope_ids(kwargs)

        if frozen is None:
            if not self._require_state_ref:
                return
            self._block(
                tool=tool,
                mode=self._on_missing,
                violation=VIOLATION_MISSING,
                message=(
                    f"StateAuthority: tool {tool!r} requires state_ref but none "
                    f"was provided (thread_id={thread_id!r}, run_id={run_id!r})."
                ),
                llm_message=(
                    f"Tool {tool!r} blocked: missing state_ref. Re-plan from "
                    "current canonical state; do not retry with a stale decision."
                ),
                side_effect_class=side_effect_class,
                thread_id=thread_id,
                decision_id=decision_id,
            )
            return

        canonical = self._get_canonical(
            tool=tool,
            thread_id=thread_id,
            run_id=run_id,
            kwargs=kwargs,
        )
        if canonical is None or canonical == "":
            if not self._require_state_ref:
                if not _RESOLVER_NONE_WARNED:
                    warnings.warn(
                        "StateAuthority: get_canonical_state_ref returned None; "
                        "skipping compare (set require_state_ref: true to fail closed).",
                        stacklevel=2,
                    )
                    _RESOLVER_NONE_WARNED = True
                return
            self._block(
                tool=tool,
                mode=self._on_missing,
                violation=VIOLATION_UNRESOLVED,
                message=(
                    f"StateAuthority: cannot resolve canonical state_ref for "
                    f"tool {tool!r} (frozen={frozen!r})."
                ),
                llm_message=(
                    f"Tool {tool!r} blocked: canonical state could not be resolved. "
                    "Refresh run state and retry from a current decision."
                ),
                side_effect_class=side_effect_class,
                thread_id=thread_id,
                decision_id=decision_id,
            )
            return

        if str(canonical) == frozen:
            return

        detail = (
            f"frozen state_ref={frozen!r} != canonical={canonical!r}"
            + (f" decision_id={decision_id!r}" if decision_id else "")
        )
        self._block(
            tool=tool,
            mode=self._on_mismatch,
            violation=VIOLATION_SUPERSEDED,
            message=(
                f"StateAuthority: tool {tool!r} derived from superseded state "
                f"({detail})."
            ),
            llm_message=(
                f"Tool {tool!r} blocked: decision was derived from superseded "
                f"state ({detail}). Re-observe current state and decide again; "
                "do not redispatch the outdated action."
            ),
            side_effect_class=side_effect_class,
            thread_id=thread_id,
            decision_id=decision_id,
            expected=str(canonical),
            actual=frozen,
        )

    def _block(
        self,
        *,
        tool: str,
        mode: str,
        violation: str,
        message: str,
        llm_message: str,
        side_effect_class: SideEffectClass | None,
        thread_id: str | None,
        decision_id: str | None,
        expected: str | None = None,
        actual: str | None = None,
    ) -> None:
        if mode == ON_MISMATCH_SOFT:
            self._emit(
                tool=tool,
                gate=GATE_SOFT_BLOCK,
                side_effect_class=side_effect_class,
                error_class="ToolBoundaryError",
                thread_id=thread_id,
                decision_id=decision_id,
            )
            raise ToolBoundaryError(
                message,
                violation=violation,
                tool_name=tool,
                llm_message=llm_message,
                field="state_ref",
                expected=expected,
                actual=actual,
                recovery_hint=(
                    "Re-plan from the current canonical state_ref; do not retry "
                    "with the frozen superseded reference."
                ),
            )

        self._emit(
            tool=tool,
            gate=GATE_HARD_BLOCK,
            side_effect_class=side_effect_class,
            error_class="LedgerHardBlockError",
            thread_id=thread_id,
            decision_id=decision_id,
        )
        raise LedgerHardBlockError(message)

    def _emit(
        self,
        *,
        tool: str,
        gate: str,
        side_effect_class: SideEffectClass | None,
        error_class: str,
        thread_id: str | None,
        decision_id: str | None,
    ) -> None:
        if self._outcome_emitter is None:
            return
        request_id = f"state:{thread_id or 'unknown'}"
        if decision_id:
            request_id = f"state:{decision_id}"
        try:
            self._outcome_emitter.emit_event(
                tool=tool,
                request_id=request_id,
                event=EVENT_RESOLUTION,
                gate=gate,
                side_effect_class=(
                    side_effect_class.value if side_effect_class is not None else None
                ),
                error_class=error_class,
            )
        except Exception:  # noqa: BLE001 — telemetry must never break the tool path
            pass


def _mark_state_authority(func: Callable[..., Any]) -> None:
    func._mycelium_state_authority = True  # type: ignore[attr-defined]


def state_authority(
    authority: StateAuthority,
    *,
    tool_name: str | None = None,
    side_effect_class: SideEffectClass | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Async decorator: run StateAuthority.check before the tool body."""

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        name = tool_name or func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            authority.check(
                name,
                args,
                dict(kwargs),
                side_effect_class=side_effect_class,
            )
            return await func(*args, **kwargs)

        _mark_state_authority(wrapper)
        return wrapper

    return decorator


def state_authority_sync(
    authority: StateAuthority,
    *,
    tool_name: str | None = None,
    side_effect_class: SideEffectClass | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Sync decorator: run StateAuthority.check before the tool body."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        name = tool_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            authority.check(
                name,
                args,
                dict(kwargs),
                side_effect_class=side_effect_class,
            )
            return func(*args, **kwargs)

        _mark_state_authority(wrapper)
        return wrapper

    return decorator


def apply_state_authority(
    func: Callable[..., Any],
    authority: StateAuthority,
    *,
    tool_name: str | None = None,
    side_effect_class: SideEffectClass | None = None,
) -> Callable[..., Any]:
    """Apply sync or async state_authority wrapper based on ``func``."""
    name = tool_name or getattr(func, "__name__", "tool")
    if inspect.iscoroutinefunction(func):
        return state_authority(
            authority,
            tool_name=name,
            side_effect_class=side_effect_class,
        )(func)
    return state_authority_sync(
        authority,
        tool_name=name,
        side_effect_class=side_effect_class,
    )(func)


__all__ = [
    "ON_MISMATCH_HARD",
    "ON_MISMATCH_MODES",
    "ON_MISMATCH_SOFT",
    "VIOLATION_MISSING",
    "VIOLATION_SUPERSEDED",
    "VIOLATION_UNRESOLVED",
    "CanonicalStateRefResolver",
    "StateAuthority",
    "apply_state_authority",
    "extract_decision_id",
    "extract_state_ref",
    "state_authority",
    "state_authority_sync",
]
