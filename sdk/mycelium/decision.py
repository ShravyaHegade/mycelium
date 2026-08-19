"""Single atomic decision point for effect-commit.

Historically policy checks were scattered across wrappers (entity destination
policy, use-time currency, authority expiry, destructive confirm, secret
protection) and evaluated at different times. A check could pass, the snapshot
could change, and the effect could still fire.

This module collapses those checks into *pure predicates* evaluated over an
``(intent, snapshot)`` pair. The resulting :class:`Decision` is recorded
atomically with the ``INTENDED -> ATTEMPTING`` transition — under the same
fenced compare-and-swap that advances the side-effect boundary — so a decision
can never drift from the write it authorized.

The composition move: new checks become plug-in predicates registered at one
enforcement point (:meth:`DecisionEngine.register`). They can never introduce a
new race because there is exactly one mutation path.

Predicates must be pure: they read the immutable ``snapshot`` and return a
verdict. No side effects, no I/O.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

__all__ = [
    "PREDICATE_AUTHORITY",
    "PREDICATE_USE_TIME_CURRENCY",
    "DecisionEngine",
    "DecisionIntent",
    "DecisionPredicate",
    "DecisionSnapshot",
    "PredicateVerdict",
    "Decision",
    "build_snapshot",
    "get_decision_engine",
    "register_decision_predicate",
    "unregister_decision_predicate",
    "reset_decision_engine",
]

PREDICATE_AUTHORITY = "authority_window"
PREDICATE_USE_TIME_CURRENCY = "use_time_currency"


@dataclass(frozen=True)
class DecisionIntent:
    """The claim context a predicate decides over.

    This is what the worker intends to do, captured at the single decision
    point: the tool, its bound call arguments, and the durable identity of the
    transition (``request_id`` / ``transition_key``).
    """

    tool: str
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    request_id: str | None = None
    transition_key: str | None = None


@dataclass(frozen=True)
class DecisionSnapshot:
    """The immutable facts a predicate reads.

    Assembled by :func:`build_snapshot` from the active-transition, authority,
    and use-time state. Predicates must not reach outside this object — that is
    what keeps them pure and the decision reproducible from the recorded row.
    """

    authority_facts: tuple[Any, ...] = ()
    use_time_facts: tuple[Any, ...] = ()
    entity_allowlist: frozenset[str] = frozenset()
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PredicateVerdict:
    """One predicate's verdict over ``(intent, snapshot)``."""

    name: str
    allowed: bool
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "allowed": self.allowed, "reason": self.reason}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PredicateVerdict:
        return cls(
            name=str(data["name"]),
            allowed=bool(data["allowed"]),
            reason=(
                str(data["reason"]) if data.get("reason") is not None else None
            ),
        )


class DecisionPredicate(Protocol):
    """A pure function ``(intent, snapshot) -> PredicateVerdict``.

    Called at the single decision point. Must not perform I/O or mutate state;
    it only reads ``snapshot``. The engine passes the predicate's registered
    name so a verdict can omit it and the engine fills it in.
    """

    def __call__(
        self, intent: DecisionIntent, snapshot: DecisionSnapshot
    ) -> PredicateVerdict | bool: ...


@dataclass(frozen=True)
class Decision:
    """The atomic result of evaluating every registered predicate.

    ``allowed`` is ``True`` only when every predicate allowed. ``verdicts``
    preserves registration order. ``denied_reasons`` collects the reasons of the
    predicates that refused, for audit and hard-block messages.
    """

    allowed: bool
    verdicts: tuple[PredicateVerdict, ...] = ()
    denied_reasons: tuple[str, ...] = ()

    @property
    def predicate_results(self) -> dict[str, bool]:
        return {verdict.name: verdict.allowed for verdict in self.verdicts}

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "verdicts": [verdict.to_dict() for verdict in self.verdicts],
            "denied_reasons": list(self.denied_reasons),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Decision:
        return cls(
            allowed=bool(data["allowed"]),
            verdicts=tuple(
                PredicateVerdict.from_dict(item)
                for item in (data.get("verdicts") or [])
            ),
            denied_reasons=tuple(str(r) for r in (data.get("denied_reasons") or [])),
        )


def _coerce_verdict(name: str, result: PredicateVerdict | bool) -> PredicateVerdict:
    if isinstance(result, PredicateVerdict):
        # Registration name wins so verdicts always carry the engine's key.
        if result.name == name:
            return result
        return PredicateVerdict(
            name=name, allowed=result.allowed, reason=result.reason
        )
    return PredicateVerdict(name=name, allowed=bool(result))


class DecisionEngine:
    """A registered set of predicates evaluated at the single decision point.

    Registration mutates under a lock; :meth:`evaluate` takes an immutable
    snapshot of the registered predicates first, so evaluation is thread- and
    async-safe and never observes a half-applied registration.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._predicates: dict[str, DecisionPredicate] = {}

    def register(self, name: str, predicate: DecisionPredicate) -> None:
        """Register (or replace) a predicate at ``name``. Evaluated in
        registration order; re-registering an existing name keeps its slot."""
        if not name:
            raise ValueError("decision predicate name must be non-empty")
        with self._lock:
            self._predicates[name] = predicate

    def unregister(self, name: str) -> None:
        with self._lock:
            self._predicates.pop(name, None)

    def registered_names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._predicates)

    def clear(self) -> None:
        with self._lock:
            self._predicates.clear()

    def evaluate(
        self, intent: DecisionIntent, snapshot: DecisionSnapshot
    ) -> Decision:
        with self._lock:
            frozen = tuple(self._predicates.items())
        verdicts: list[PredicateVerdict] = []
        denied: list[str] = []
        for name, predicate in frozen:
            verdict = _coerce_verdict(name, predicate(intent, snapshot))
            verdicts.append(verdict)
            if not verdict.allowed:
                denied.append(verdict.reason or name)
        return Decision(
            allowed=all(verdict.allowed for verdict in verdicts),
            verdicts=tuple(verdicts),
            denied_reasons=tuple(denied),
        )


_engine = DecisionEngine()


def get_decision_engine() -> DecisionEngine:
    """Return the process-wide decision engine used at the single point."""
    return _engine


def register_decision_predicate(name: str, predicate: DecisionPredicate) -> None:
    """Public hook: register an extra predicate at the single decision point.

    The predicate is evaluated over ``(intent, snapshot)`` at the
    ``INTENDED -> ATTEMPTING`` transition and its verdict is recorded atomically
    with that fenced write.
    """
    _engine.register(name, predicate)


def unregister_decision_predicate(name: str) -> None:
    _engine.unregister(name)


def reset_decision_engine() -> None:
    """Reset to only the built-in authority + currency predicates.

    Drops any host-registered predicates and re-installs the built-ins so the
    final-boundary decision stays wired. Intended for test isolation.
    """
    _engine.clear()
    _register_builtin_predicates(_engine)


def build_snapshot(
    intent: DecisionIntent | None = None,
    *,
    authority_decision: Any = None,
    currency_decision: Any = None,
) -> DecisionSnapshot:
    """Assemble the current facts snapshot for predicate evaluation.

    Reads the pending authority and use-time state so predicates stay pure —
    they receive the facts here rather than re-reading module state themselves.
    Deferred imports avoid an import cycle with the ledger.

    ``authority_decision`` / ``currency_decision`` carry the validation results
    already produced by the final-boundary enforcement so the built-in
    predicates report the *same* verdict without re-running (and possibly
    re-raising) those checks — no double-enforcement.
    """
    from mycelium.authority_window import _pending_var as _authority_pending
    from mycelium.use_time_currency import _pending_var as _use_time_pending

    return DecisionSnapshot(
        authority_facts=tuple(_authority_pending.get()),
        use_time_facts=tuple(_use_time_pending.get()),
        extra={
            "authority_decision": authority_decision,
            "currency_decision": currency_decision,
        },
    )


def _verdict_from_validation(name: str, validation: Any) -> PredicateVerdict:
    """Translate an authority/currency validation result into a verdict.

    The final-boundary enforcement raises on denial, so by the time a validation
    object reaches here it is ``allowed`` or ``skipped`` (both permit the
    effect). A missing validation (check not wired for this tool) is a permit.
    """
    if validation is None:
        return PredicateVerdict(name=name, allowed=True, reason=None)
    decision = getattr(validation, "decision", None)
    reason = getattr(validation, "reason", None)
    allowed = decision in {"allowed", "skipped"}
    return PredicateVerdict(name=name, allowed=allowed, reason=None if allowed else reason)


def _authority_predicate(
    intent: DecisionIntent, snapshot: DecisionSnapshot
) -> PredicateVerdict:
    return _verdict_from_validation(
        PREDICATE_AUTHORITY, snapshot.extra.get("authority_decision")
    )


def _use_time_currency_predicate(
    intent: DecisionIntent, snapshot: DecisionSnapshot
) -> PredicateVerdict:
    return _verdict_from_validation(
        PREDICATE_USE_TIME_CURRENCY, snapshot.extra.get("currency_decision")
    )


def _register_builtin_predicates(engine: DecisionEngine) -> None:
    engine.register(PREDICATE_AUTHORITY, _authority_predicate)
    engine.register(PREDICATE_USE_TIME_CURRENCY, _use_time_currency_predicate)


_register_builtin_predicates(_engine)
