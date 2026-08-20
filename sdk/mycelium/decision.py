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

import functools
import inspect
import threading
from collections.abc import Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Protocol

__all__ = [
    "PREDICATE_AUTHORITY",
    "PREDICATE_DESTRUCTIVE_CONFIRM",
    "PREDICATE_DESTINATION_POLICY",
    "PREDICATE_SECRET_PROTECTION",
    "PREDICATE_USE_TIME_CURRENCY",
    "DecisionPolicyBundle",
    "DecisionEngine",
    "DecisionIntent",
    "DecisionPredicate",
    "DecisionSnapshot",
    "PredicateVerdict",
    "PolicyFact",
    "Decision",
    "build_snapshot",
    "apply_decision_policy",
    "get_decision_evidence",
    "get_policy_blocked_error",
    "get_decision_engine",
    "register_decision_predicate",
    "unregister_decision_predicate",
    "reset_decision_engine",
]

PREDICATE_AUTHORITY = "authority_window"
PREDICATE_DESTINATION_POLICY = "destination_policy"
PREDICATE_DESTRUCTIVE_CONFIRM = "destructive_confirm"
PREDICATE_SECRET_PROTECTION = "secret_protection"
PREDICATE_USE_TIME_CURRENCY = "use_time_currency"


@dataclass(frozen=True)
class PolicyFact:
    """A safe, immutable result captured before the atomic decision.

    Facts contain only an allow/deny bit and a non-sensitive reason. They never
    contain argument values, secret material, or mutable policy objects.
    """

    name: str
    allowed: bool
    reason: str | None = None


@dataclass(frozen=True)
class DecisionPolicyBundle:
    """Policies whose facts are composed at the ledger decision point.

    ``Any`` is intentional here: keeping the container policy-module agnostic
    avoids importing policy implementations into the pure predicate layer.
    """

    entity_policy: Any = None
    destructive_policy: Any = None
    destructive_store: Any = None
    secret_policy: Any = None
    secret_fields: frozenset[str] = frozenset()
    consequential: bool = False
    use_time_policy: Any = None


@dataclass(frozen=True)
class _PendingDestructiveCheck:
    func: Any
    tool: str
    args: tuple[Any, ...]
    kwargs: Mapping[str, Any]
    policy: Any
    store: Any


_policy_facts_var: ContextVar[tuple[PolicyFact, ...]] = ContextVar(
    "mycelium_atomic_policy_facts", default=()
)
_policy_blocked_var: ContextVar[Exception | None] = ContextVar(
    "mycelium_atomic_policy_blocked", default=None
)
_pending_destructive_var: ContextVar[_PendingDestructiveCheck | None] = ContextVar(
    "mycelium_atomic_pending_destructive", default=None
)
_sanitize_intent_var: ContextVar[bool] = ContextVar(
    "mycelium_atomic_sanitize_intent", default=False
)


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
    policy_facts: tuple[PolicyFact, ...] = ()
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
        if not isinstance(data, Mapping):
            raise TypeError("predicate verdict must be a mapping")
        if not isinstance(data.get("name"), str) or not data["name"]:
            raise ValueError("predicate verdict name must be a non-empty string")
        if not isinstance(data.get("allowed"), bool):
            raise ValueError("predicate verdict allowed must be a boolean")
        reason = data.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise ValueError("predicate verdict reason must be a string or null")
        return cls(
            name=data["name"],
            allowed=data["allowed"],
            reason=reason,
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
        if not isinstance(data, Mapping):
            raise TypeError("decision must be a mapping")
        if set(data) != {"allowed", "verdicts", "denied_reasons"}:
            raise ValueError("decision must contain allowed, verdicts, and denied_reasons")
        if not isinstance(data["allowed"], bool):
            raise ValueError("decision allowed must be a boolean")
        if not isinstance(data["verdicts"], (list, tuple)):
            raise ValueError("decision verdicts must be a list")
        if not isinstance(data["denied_reasons"], (list, tuple)) or any(
            not isinstance(reason, str) for reason in data["denied_reasons"]
        ):
            raise ValueError("decision denied_reasons must be a list of strings")
        verdicts = tuple(
            PredicateVerdict.from_dict(item) for item in data["verdicts"]
        )
        allowed = data["allowed"]
        if verdicts and allowed != all(verdict.allowed for verdict in verdicts):
            raise ValueError("decision allowed conflicts with predicate verdicts")
        if allowed and data["denied_reasons"]:
            raise ValueError("allowed decision cannot contain denied reasons")
        return cls(
            allowed=allowed,
            verdicts=verdicts,
            denied_reasons=tuple(data["denied_reasons"]),
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
    """Reset to only the five built-in policy predicates.

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

    policy_facts = list(_policy_facts_var.get())
    pending_destructive = _pending_destructive_var.get()
    if pending_destructive is not None:
        from mycelium.destructive_confirm import (
            DestructiveGrantError,
            enforce_destructive_confirm,
        )

        try:
            _, _, destructive_decision = enforce_destructive_confirm(
                pending_destructive.tool,
                pending_destructive.args,
                dict(pending_destructive.kwargs),
                policy=pending_destructive.policy,
                func=pending_destructive.func,
                store=pending_destructive.store,
            )
            policy_facts.append(
                _fact(
                    PREDICATE_DESTRUCTIVE_CONFIRM,
                    allowed=destructive_decision.decision in {"allow", "allowed"},
                    reason=destructive_decision.reason,
                )
            )
        except DestructiveGrantError as exc:
            _policy_blocked_var.set(exc)
            policy_facts.append(
                _fact(
                    PREDICATE_DESTRUCTIVE_CONFIRM,
                    allowed=False,
                    reason=exc.reason,
                )
            )

    return DecisionSnapshot(
        authority_facts=tuple(_authority_pending.get()),
        use_time_facts=tuple(_use_time_pending.get()),
        policy_facts=tuple(policy_facts),
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


def _captured_policy_verdict(
    name: str, snapshot: DecisionSnapshot
) -> PredicateVerdict | None:
    fact = next((item for item in snapshot.policy_facts if item.name == name), None)
    if fact is None:
        return None
    return PredicateVerdict(name=name, allowed=fact.allowed, reason=fact.reason)


def _policy_predicate(name: str) -> DecisionPredicate:
    def predicate(
        intent: DecisionIntent, snapshot: DecisionSnapshot
    ) -> PredicateVerdict:
        del intent
        return _captured_policy_verdict(name, snapshot) or PredicateVerdict(
            name=name, allowed=True
        )

    return predicate


def _authority_predicate(
    intent: DecisionIntent, snapshot: DecisionSnapshot
) -> PredicateVerdict:
    captured = _captured_policy_verdict(PREDICATE_AUTHORITY, snapshot)
    if captured is not None and not captured.allowed:
        return captured
    return _verdict_from_validation(
        PREDICATE_AUTHORITY, snapshot.extra.get("authority_decision")
    )


def _use_time_currency_predicate(
    intent: DecisionIntent, snapshot: DecisionSnapshot
) -> PredicateVerdict:
    captured = _captured_policy_verdict(PREDICATE_USE_TIME_CURRENCY, snapshot)
    if captured is not None and not captured.allowed:
        return captured
    return _verdict_from_validation(
        PREDICATE_USE_TIME_CURRENCY, snapshot.extra.get("currency_decision")
    )


def _register_builtin_predicates(engine: DecisionEngine) -> None:
    engine.register(
        PREDICATE_DESTINATION_POLICY,
        _policy_predicate(PREDICATE_DESTINATION_POLICY),
    )
    engine.register(
        PREDICATE_DESTRUCTIVE_CONFIRM,
        _policy_predicate(PREDICATE_DESTRUCTIVE_CONFIRM),
    )
    engine.register(
        PREDICATE_SECRET_PROTECTION,
        _policy_predicate(PREDICATE_SECRET_PROTECTION),
    )
    engine.register(PREDICATE_AUTHORITY, _authority_predicate)
    engine.register(PREDICATE_USE_TIME_CURRENCY, _use_time_currency_predicate)


def get_policy_blocked_error() -> Exception | None:
    """Return the original public policy error captured for this call."""
    return _policy_blocked_var.get()


def get_decision_evidence(
    args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    if not _sanitize_intent_var.get():
        return tuple(args), dict(kwargs)
    from mycelium.secret_protection import get_active_secret_policy, sanitize_secrets

    policy = get_active_secret_policy()
    safe_args, safe_kwargs = sanitize_secrets(
        (tuple(args), dict(kwargs)),
        entropy_detection=policy.entropy_detection if policy is not None else True,
        allow_fields=policy.allow_fields if policy is not None else frozenset(),
    )
    return tuple(safe_args), dict(safe_kwargs)


def _fact(name: str, *, allowed: bool, reason: str | None = None) -> PolicyFact:
    return PolicyFact(name=name, allowed=allowed, reason=reason)


def _prepare_policy_call(
    func: Any,
    name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    bundle: DecisionPolicyBundle,
) -> tuple[
    tuple[Any, ...],
    dict[str, Any],
    list[PolicyFact],
    Exception | None,
    list[tuple[Any, Token[Any]]],
]:
    """Capture policy facts without making an execution decision."""
    from mycelium.authority_window import _pending_var as authority_pending
    from mycelium.destructive_confirm import (
        reset_active_destructive_policy,
        reset_destructive_grant_store,
        set_active_destructive_policy,
        set_destructive_grant_store,
    )
    from mycelium.destructive_confirm import (
        _decision_var as destructive_decision,
    )
    from mycelium.entity_guard import (
        EntityDecision,
        EntityGuardError,
        enforce_entity_guard,
        reset_active_entity_policy,
        set_active_entity_policy,
    )
    from mycelium.entity_guard import (
        _decision_var as entity_decision,
    )
    from mycelium.secret_protection import (
        SecretInArgsError,
        enforce_secret_args,
        reset_active_secret_policy,
        sanitize_secrets,
        scan_secrets,
        set_active_secret_policy,
    )
    from mycelium.use_time_currency import (
        UseTimeCurrencyError,
        authorize_use_time_facts,
        reset_use_time_currency_policy,
        set_use_time_currency_policy,
    )
    from mycelium.use_time_currency import (
        _pending_var as currency_pending,
    )

    cleanup: list[tuple[Any, Token[Any]]] = []
    cleanup.append((authority_pending, authority_pending.set(())))
    cleanup.append((currency_pending, currency_pending.set(())))
    cleanup.append((entity_decision, entity_decision.set(None)))
    cleanup.append((destructive_decision, destructive_decision.set(None)))
    cleanup.append((_pending_destructive_var, _pending_destructive_var.set(None)))
    cleanup.append((_sanitize_intent_var, _sanitize_intent_var.set(False)))
    if bundle.secret_policy is not None:
        cleanup.append(
            (reset_active_secret_policy, set_active_secret_policy(bundle.secret_policy))
        )
    if bundle.entity_policy is not None:
        cleanup.append(
            (reset_active_entity_policy, set_active_entity_policy(bundle.entity_policy))
        )
    if bundle.destructive_policy is not None:
        cleanup.append(
            (
                reset_active_destructive_policy,
                set_active_destructive_policy(bundle.destructive_policy),
            )
        )
        if bundle.destructive_store is not None:
            cleanup.append(
                (
                    reset_destructive_grant_store,
                    set_destructive_grant_store(bundle.destructive_store),
                )
            )
    if bundle.use_time_policy is not None:
        cleanup.append(
            (
                reset_use_time_currency_policy,
                set_use_time_currency_policy(bundle.use_time_policy),
            )
        )

    call_args, call_kwargs = args, dict(kwargs)
    facts: list[PolicyFact] = []
    blocked: Exception | None = None

    if bundle.secret_policy is not None:
        findings = scan_secrets(
            {"args": call_args, "kwargs": call_kwargs},
            entropy_detection=bundle.secret_policy.entropy_detection,
            allow_fields=bundle.secret_policy.allow_fields,
        )
        if any(finding.kind != "reference" for finding in findings):
            _sanitize_intent_var.set(True)
        try:
            call_args, call_kwargs = enforce_secret_args(
                name,
                call_args,
                call_kwargs,
                policy=bundle.secret_policy,
                secret_fields=bundle.secret_fields,
                consequential=bundle.consequential,
            )
            facts.append(_fact(PREDICATE_SECRET_PROTECTION, allowed=True))
        except SecretInArgsError as exc:
            blocked = blocked or exc
            call_args, call_kwargs = sanitize_secrets(
                (call_args, call_kwargs),
                entropy_detection=bundle.secret_policy.entropy_detection,
                allow_fields=bundle.secret_policy.allow_fields,
            )
            call_args = tuple(call_args)
            call_kwargs = dict(call_kwargs)
            reason = ",".join(exc.kinds) if exc.kinds else "raw secret material"
            facts.append(
                _fact(PREDICATE_SECRET_PROTECTION, allowed=False, reason=reason)
            )

    if bundle.entity_policy is not None:
        try:
            call_args, call_kwargs, _ = enforce_entity_guard(
                name,
                call_args,
                call_kwargs,
                policy=bundle.entity_policy,
                func=func,
            )
            facts.append(_fact(PREDICATE_DESTINATION_POLICY, allowed=True))
        except EntityGuardError as exc:
            blocked = blocked or exc
            entity_decision.set(
                EntityDecision(
                    tool=name,
                    destinations=(),
                    policy_version=bundle.entity_policy.policy_version,
                    decision="deny",
                    reason=exc.reason,
                )
            )
            facts.append(
                _fact(PREDICATE_DESTINATION_POLICY, allowed=False, reason=exc.reason)
            )

    if bundle.use_time_policy is not None:
        try:
            authorize_use_time_facts(
                name,
                call_args,
                call_kwargs,
                policy=bundle.use_time_policy,
                func=func,
            )
            facts.append(_fact(PREDICATE_USE_TIME_CURRENCY, allowed=True))
        except UseTimeCurrencyError as exc:
            blocked = blocked or exc
            facts.append(
                _fact(PREDICATE_USE_TIME_CURRENCY, allowed=False, reason=exc.reason)
            )

    if bundle.destructive_policy is not None:
        if blocked is not None:
            facts.append(
                _fact(
                    PREDICATE_DESTRUCTIVE_CONFIRM,
                    allowed=True,
                    reason="skipped after prior denial",
                )
            )
        else:
            _pending_destructive_var.set(
                _PendingDestructiveCheck(
                    func=func,
                    tool=name,
                    args=call_args,
                    kwargs=dict(call_kwargs),
                    policy=bundle.destructive_policy,
                    store=bundle.destructive_store,
                )
            )
    return call_args, call_kwargs, facts, blocked, cleanup


def _reset_policy_context(cleanup: list[tuple[Any, Token[Any]]]) -> None:
    for resetter, token in reversed(cleanup):
        if hasattr(resetter, "reset"):
            resetter.reset(token)
        else:
            resetter(token)


def apply_decision_policy(
    func: Any,
    bundle: DecisionPolicyBundle,
    *,
    tool_name: str | None = None,
) -> Any:
    """Compose policy fact capture around a ledgered tool.

    This wrapper deliberately never authorizes the body. It captures immutable
    facts and an optional compatibility exception, then always enters the
    ledger. The ledger evaluates the pure predicates and CAS-records their
    result with ``INTENDED -> ATTEMPTING`` before the exception can be raised or
    the body can execute.
    """
    name = tool_name or getattr(func, "__name__", "tool")

    async def prepare_and_call(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        call_args, call_kwargs, facts, blocked, cleanup = _prepare_policy_call(
            func, name, args, kwargs, bundle
        )
        facts_token = _policy_facts_var.set(tuple(facts))
        blocked_token = _policy_blocked_var.set(blocked)
        try:
            return await func(*call_args, **call_kwargs)
        finally:
            _policy_blocked_var.reset(blocked_token)
            _policy_facts_var.reset(facts_token)
            _reset_policy_context(cleanup)

    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await prepare_and_call(tuple(args), dict(kwargs))

        async_wrapper._mycelium_atomic_decision_policy = True  # type: ignore[attr-defined]
        return async_wrapper

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        call_args, call_kwargs, facts, blocked, cleanup = _prepare_policy_call(
            func, name, tuple(args), dict(kwargs), bundle
        )
        facts_token = _policy_facts_var.set(tuple(facts))
        blocked_token = _policy_blocked_var.set(blocked)
        try:
            return func(*call_args, **call_kwargs)
        finally:
            _policy_blocked_var.reset(blocked_token)
            _policy_facts_var.reset(facts_token)
            _reset_policy_context(cleanup)

    sync_wrapper._mycelium_atomic_decision_policy = True  # type: ignore[attr-defined]
    return sync_wrapper


_register_builtin_predicates(_engine)
