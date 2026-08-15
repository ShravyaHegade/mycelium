"""Use-time currency (AF-012): revalidate decide-time facts before side effects.

A fact that was current when the agent decided must not authorize a
consequential side effect if it is stale, changed, missing, unverifiable,
or outside its freshness window at execute time.

Pair with authority-window expiry (batch item 4). Neither weakens the other.
Prompt scanning is not used — only host-controlled facts and validators.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ParamSpec, TypeVar

from mycelium.authority_window import (
    PHASE_AUTHORIZE,
    PHASE_USE,
    AuthorityValidationPhase,
    enforce_pending_authorities_at_use,
    ensure_aware_utc,
    utc_now,
)
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import get_active_dispatch_id, get_active_execution_scope

P = ParamSpec("P")
R = TypeVar("R")

MISSING_POLICY_ERROR = "error"
MISSING_POLICY_WARN = "warn"
MISSING_POLICIES = frozenset({MISSING_POLICY_ERROR, MISSING_POLICY_WARN})

DECISION_ALLOWED = "allowed"
DECISION_DENIED = "denied"
DECISION_SKIPPED = "skipped"

REASON_VALID = "valid"
REASON_STALE = "stale"
REASON_CHANGED = "changed"
REASON_CONDITION_FALSE = "condition_false"
REASON_REVISION_MISMATCH = "revision_mismatch"
REASON_SUBJECT_MISMATCH = "subject_mismatch"
REASON_TENANT_MISMATCH = "tenant_mismatch"
REASON_ACCOUNT_MISMATCH = "account_mismatch"
REASON_POLICY_CHANGED = "policy_changed"
REASON_VALIDATOR_MISSING = "validator_missing"
REASON_VALIDATOR_FAILED = "validator_failed"
REASON_VALIDATOR_TIMEOUT = "validator_timeout"
REASON_UNVERIFIABLE = "unverifiable"
REASON_MISSING = "missing"
REASON_MALFORMED = "malformed"
REASON_DISABLED = "disabled"
REASON_TIMELESS = "timeless"

_MISSING = object()

ValidatorFn = Callable[..., "ValidatorResult | Awaitable[ValidatorResult]"]


class UseTimeCurrencyError(ToolBoundaryError):
    """A required use-time fact is stale, changed, missing, or unverifiable."""

    def __init__(
        self,
        message: str,
        *,
        tool: str | None = None,
        fact_name: str | None = None,
        reason: str = REASON_UNVERIFIABLE,
        phase: str = PHASE_USE,
        subject_ref: str | None = None,
    ) -> None:
        super().__init__(
            message,
            violation="use_time_currency",
            tool_name=tool or "tool",
            llm_message=(
                f"Use-time currency blocked {(tool or 'tool')!r}: {reason}. "
                "Re-authorize with a fresh host fact. The tool body was not "
                "executed and the side-effect boundary was not crossed."
            ),
            field=phase,
            expected="current_fact",
            recovery_hint=(
                "Capture a fresh host fact via use_time_facts.capture and ensure "
                "the registered validator returns current truth. Mycelium does "
                "not refresh stale decide-time facts automatically."
            ),
        )
        self.tool = tool
        self.fact_name = fact_name
        self.reason = reason
        self.phase = phase
        self.subject_ref = subject_ref


@dataclass(frozen=True)
class UseTimeFact:
    """Host-controlled decide-time authorization fact. Never model-minted."""

    name: str
    subject_type: str
    subject_id: str
    observed_at: datetime
    tool: str | None = None
    tenant: str | None = None
    account: str | None = None
    value_digest: str | None = None
    revision: str | None = None
    max_age_seconds: float | None = None
    request_id: str | None = None
    run_id: str | None = None
    thread_id: str | None = None
    policy_version: str | None = None
    validator: str | None = None
    provenance: str | None = None
    provider_precondition: str | None = None
    require_value_digest: str | None = None
    compare_to_arg: str | None = None
    bind_request_id: bool = False
    bind_run_id: bool = False
    bind_thread_id: bool = False

    def __post_init__(self) -> None:
        ensure_aware_utc(self.observed_at, field="observed_at")
        if not self.name or not str(self.name).strip():
            raise ValueError("use-time fact name must be non-empty")
        if not self.subject_type or not str(self.subject_type).strip():
            raise ValueError("use-time fact subject_type must be non-empty")
        if not self.subject_id or not str(self.subject_id).strip():
            raise ValueError("use-time fact subject_id must be non-empty")
        if self.max_age_seconds is not None and self.max_age_seconds < 0:
            raise ValueError("max_age_seconds must be >= 0")

    @property
    def subject_ref(self) -> str:
        return f"{self.subject_type}:{_safe_digest(self.subject_id)}"


@dataclass(frozen=True)
class UseTimeValidation:
    """Payload-free authorize/use decision for a fact."""

    decision: str
    reason: str
    phase: str
    fact_name: str | None = None
    tool: str | None = None
    subject_ref: str | None = None
    tenant: str | None = None
    account: str | None = None
    decide_revision: str | None = None
    use_revision: str | None = None
    observed_at: datetime | None = None
    max_age_seconds: float | None = None
    validator: str | None = None
    policy_version: str | None = None
    provider_precondition: str | None = None
    provider_precondition_present: bool | None = None
    request_id: str | None = None
    run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reason": self.reason,
            "phase": self.phase,
            "fact_name": self.fact_name,
            "tool": self.tool,
            "subject_ref": self.subject_ref,
            "tenant": self.tenant,
            "account": self.account,
            "decide_revision": self.decide_revision,
            "use_revision": self.use_revision,
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "max_age_seconds": self.max_age_seconds,
            "validator": self.validator,
            "policy_version": self.policy_version,
            "provider_precondition": self.provider_precondition,
            "provider_precondition_present": self.provider_precondition_present,
            "request_id": self.request_id,
            "run_id": self.run_id,
        }


@dataclass(frozen=True)
class ValidatorResult:
    """Host validator output. Must not create the side effect being authorized."""

    current: bool
    reason: str = REASON_VALID
    revision: str | None = None
    observed_at: datetime | None = None
    value: Any = None
    value_digest: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def digest(self) -> str | None:
        if self.value_digest is not None:
            return self.value_digest
        if self.value is None:
            return None
        return value_digest(self.value)


@dataclass(frozen=True)
class UseTimeFactSpec:
    """Host-declared fact requirement for a tool."""

    name: str
    subject_type: str
    id_from: str
    validator: str
    tenant_from: str | None = None
    account_from: str | None = None
    require: Mapping[str, Any] | None = None
    revision_from: str | None = None
    max_age_seconds: float | None = None
    bind_request_id: bool = False
    bind_run_id: bool = False
    bind_thread_id: bool = False
    compare_to_arg: str | None = None
    provider_precondition: str | None = None

    def __post_init__(self) -> None:
        if not self.name or not str(self.name).strip():
            raise ValueError("fact name must be non-empty")
        if not self.subject_type or not str(self.subject_type).strip():
            raise ValueError("subject.type must be non-empty")
        if not self.id_from or not str(self.id_from).strip():
            raise ValueError("subject.id_from must be non-empty")
        if not self.validator or not str(self.validator).strip():
            raise ValueError("validator must be non-empty")
        if self.max_age_seconds is not None and self.max_age_seconds < 0:
            raise ValueError("max_age_seconds must be >= 0")


@dataclass(frozen=True)
class UseTimeToolPolicy:
    facts: tuple[UseTimeFactSpec, ...] = ()


@dataclass(frozen=True)
class UseTimeCurrencyPolicy:
    """Host-owned use-time currency configuration."""

    enabled: bool = True
    missing_policy: str = MISSING_POLICY_ERROR
    policy_version: str = "unspecified"
    tools: dict[str, UseTimeToolPolicy] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.missing_policy not in MISSING_POLICIES:
            raise ValueError(
                "use_time_currency.missing_policy must be one of "
                f"{sorted(MISSING_POLICIES)}, got {self.missing_policy!r}"
            )


_clock_var: ContextVar[Callable[[], float] | None] = ContextVar(
    "mycelium_use_time_clock", default=None
)
_policy_var: ContextVar[UseTimeCurrencyPolicy | None] = ContextVar(
    "mycelium_use_time_policy", default=None
)
_captured_var: ContextVar[tuple[UseTimeFact, ...]] = ContextVar(
    "mycelium_use_time_captured", default=()
)
_pending_var: ContextVar[tuple[UseTimeFact, ...]] = ContextVar(
    "mycelium_use_time_pending", default=()
)
_decision_var: ContextVar[tuple[UseTimeValidation, ...]] = ContextVar(
    "mycelium_use_time_decisions", default=()
)
_validators: dict[str, ValidatorFn] = {}
_validator_timeouts: dict[str, float] = {}


def value_digest(value: Any) -> str:
    """Stable digest for comparison — never stores raw sensitive values."""
    try:
        text = repr(value)
    except Exception:
        text = f"<unrepr:{type(value).__name__}>"
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _safe_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def subject_ref(subject_type: str, subject_id: str) -> str:
    return f"{subject_type}:{_safe_digest(subject_id)}"


def set_use_time_clock(clock: Callable[[], float] | None) -> Token[Callable[[], float] | None]:
    return _clock_var.set(clock)


def reset_use_time_clock(token: Token[Callable[[], float] | None]) -> None:
    _clock_var.reset(token)


def use_time_now() -> datetime:
    """Host/infrastructure UTC clock. Never accepts model-provided time."""
    clock = _clock_var.get()
    if clock is not None:
        return datetime.fromtimestamp(float(clock()), tz=UTC)
    # Share authority clock when installed so tests inject once.
    return utc_now()


def get_use_time_currency_policy() -> UseTimeCurrencyPolicy | None:
    return _policy_var.get()


def set_use_time_currency_policy(
    policy: UseTimeCurrencyPolicy | None,
) -> Token[UseTimeCurrencyPolicy | None]:
    return _policy_var.set(policy)


def reset_use_time_currency_policy(token: Token[UseTimeCurrencyPolicy | None]) -> None:
    _policy_var.reset(token)


def get_pending_use_time_facts() -> tuple[UseTimeFact, ...]:
    return _pending_var.get()


def get_use_time_decisions() -> tuple[UseTimeValidation, ...]:
    return _decision_var.get()


def get_captured_use_time_facts() -> tuple[UseTimeFact, ...]:
    return _captured_var.get()


def clear_pending_use_time_facts() -> None:
    _pending_var.set(())


def clear_captured_use_time_facts() -> None:
    _captured_var.set(())


def reset_use_time_currency_state() -> None:
    _clock_var.set(None)
    _policy_var.set(None)
    _captured_var.set(())
    _pending_var.set(())
    _decision_var.set(())
    _validators.clear()
    _validator_timeouts.clear()


def register_use_time_validator(
    name: str,
    fn: ValidatorFn,
    *,
    timeout_seconds: float | None = None,
) -> None:
    """Register a host validator. Never selected by the model."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("validator name must be a non-empty string")
    if not callable(fn):
        raise ValueError("validator must be callable")
    key = name.strip()
    _validators[key] = fn
    if timeout_seconds is not None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        _validator_timeouts[key] = float(timeout_seconds)
    elif key in _validator_timeouts:
        del _validator_timeouts[key]


def registered_use_time_validators() -> frozenset[str]:
    return frozenset(_validators)


class _UseTimeFactsAPI:
    """Host API for capturing decide-time facts. Rejects model-controlled records."""

    def capture(
        self,
        *,
        name: str,
        subject_type: str,
        subject_id: str,
        value: Any = None,
        revision: str | None = None,
        observed_at: datetime | None = None,
        max_age_seconds: float | None = None,
        request_id: str | None = None,
        run_id: str | None = None,
        thread_id: str | None = None,
        tenant: str | None = None,
        account: str | None = None,
        policy_version: str | None = None,
        validator: str | None = None,
        provenance: str | None = None,
        provider_precondition: str | None = None,
        tool: str | None = None,
        require_value: Any = _MISSING,
        compare_to_arg: str | None = None,
    ) -> UseTimeFact:
        if isinstance(subject_id, Mapping):
            raise UseTimeCurrencyError(
                "use-time fact subject must be host-controlled, not a mapping",
                fact_name=name,
                reason=REASON_MALFORMED,
                phase=PHASE_AUTHORIZE,
            )
        observed = (
            ensure_aware_utc(observed_at, field="observed_at")
            if observed_at is not None
            else use_time_now()
        )
        require_digest = None if require_value is _MISSING else value_digest(require_value)
        fact = UseTimeFact(
            name=str(name).strip(),
            subject_type=str(subject_type).strip(),
            subject_id=str(subject_id).strip(),
            observed_at=observed,
            tool=tool,
            tenant=str(tenant).strip() if tenant is not None else None,
            account=str(account).strip() if account is not None else None,
            value_digest=value_digest(value) if value is not None else None,
            revision=str(revision) if revision is not None else None,
            max_age_seconds=float(max_age_seconds) if max_age_seconds is not None else None,
            request_id=request_id,
            run_id=run_id,
            thread_id=thread_id,
            policy_version=policy_version,
            validator=validator,
            provenance=provenance or "host_capture",
            provider_precondition=provider_precondition,
            require_value_digest=require_digest,
            compare_to_arg=compare_to_arg,
        )
        current = _captured_var.get()
        kept = tuple(
            item
            for item in current
            if _fact_identity(item) != _fact_identity(fact)
        )
        _captured_var.set((*kept, fact))
        return fact


use_time_facts = _UseTimeFactsAPI()


def _fact_identity(fact: UseTimeFact) -> tuple[str | None, ...]:
    return (
        fact.name,
        fact.subject_type,
        fact.subject_id,
        fact.tenant,
        fact.account,
    )


def _pending_fact_identity(fact: UseTimeFact) -> tuple[Any, ...]:
    return (
        *_fact_identity(fact),
        fact.tool,
        fact.validator,
        fact.value_digest,
        fact.require_value_digest,
        fact.compare_to_arg,
        fact.revision,
        fact.max_age_seconds,
        fact.provider_precondition,
        fact.policy_version,
        fact.request_id,
        fact.run_id,
        fact.thread_id,
        fact.bind_request_id,
        fact.bind_run_id,
        fact.bind_thread_id,
    )


def _append_decision(decision: UseTimeValidation) -> UseTimeValidation:
    current = _decision_var.get()
    _decision_var.set((*current, decision))
    return decision


def _lookup_path(mapping: Mapping[str, Any], path: str) -> Any:
    current: Any = mapping
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return _MISSING
    return current


def _bound_mapping(
    func: Callable[..., Any] | None, args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if func is None:
        return dict(kwargs)
    try:
        signature = inspect.signature(func)
        bound = signature.bind_partial(*args)
        mapping.update(bound.arguments)
        mapping.update(kwargs)
        for name, parameter in signature.parameters.items():
            if parameter.default is not inspect.Parameter.empty:
                mapping.setdefault(name, parameter.default)
    except (TypeError, ValueError):
        mapping.update(kwargs)
    return mapping


def _call_ids(kwargs: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    """Resolve context IDs for AUTHORIZE (call args / active scope as fallback)."""
    request_id = kwargs.get("request_id")
    if not isinstance(request_id, str) or not request_id.strip():
        request_id = get_active_dispatch_id()
    scope = get_active_execution_scope()
    run_id = kwargs.get("run_id") or (scope.run_id if scope else None)
    thread_id = kwargs.get("thread_id") or (scope.thread_id if scope else None)
    if run_id is not None:
        run_id = str(run_id)
    if thread_id is not None:
        thread_id = str(thread_id)
    return request_id, run_id, thread_id


def _current_context_ids(
    kwargs: Mapping[str, Any],
) -> tuple[str | None, str | None, str | None]:
    """Resolve trusted current context for USE-phase binding checks.

    Prefer active dispatch / execution scope over authorize-time call mapping so
    a ``dispatch_scope`` or ``execution_scope`` switch before the side-effect
    boundary fails closed. Fall back to call kwargs only when no active context
    is set (tools that pass ids as arguments without scopes).
    """
    request_id = get_active_dispatch_id()
    if not isinstance(request_id, str) or not request_id.strip():
        rid = kwargs.get("request_id")
        request_id = rid if isinstance(rid, str) and rid.strip() else None
    scope = get_active_execution_scope()
    run_id = (scope.run_id if scope is not None else None) or kwargs.get("run_id")
    thread_id = (scope.thread_id if scope is not None else None) or kwargs.get(
        "thread_id"
    )
    if run_id is not None:
        run_id = str(run_id)
    if thread_id is not None:
        thread_id = str(thread_id)
    return request_id, run_id, thread_id


def _enforce_context_bindings(
    spec: UseTimeFactSpec,
    match: UseTimeFact,
    *,
    tool: str,
    request_id: str | None,
    run_id: str | None,
    thread_id: str | None,
    policy_version: str | None,
) -> None:
    for enabled, captured, current in (
        (spec.bind_request_id, match.request_id, request_id),
        (spec.bind_run_id, match.run_id, run_id),
        (spec.bind_thread_id, match.thread_id, thread_id),
    ):
        if not enabled:
            continue
        reason = (
            REASON_MISSING
            if captured is None or current is None
            else REASON_SUBJECT_MISMATCH
        )
        if captured is None or current is None or str(captured) != str(current):
            _raise_currency(
                tool=tool,
                fact_name=match.name,
                reason=reason,
                phase=PHASE_AUTHORIZE,
                subject_ref=match.subject_ref,
                decide_revision=match.revision,
                policy_version=policy_version,
                request_id=request_id,
                run_id=run_id,
                tenant=match.tenant,
                account=match.account,
            )


def _enforce_context_bindings_at_use(
    fact: UseTimeFact, kwargs: Mapping[str, Any]
) -> None:
    request_id, run_id, thread_id = _current_context_ids(kwargs)
    for enabled, captured, current in (
        (fact.bind_request_id, fact.request_id, request_id),
        (fact.bind_run_id, fact.run_id, run_id),
        (fact.bind_thread_id, fact.thread_id, thread_id),
    ):
        if not enabled:
            continue
        reason = (
            REASON_MISSING
            if captured is None or current is None
            else REASON_SUBJECT_MISMATCH
        )
        if captured is None or current is None or str(captured) != str(current):
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=reason,
                phase=PHASE_USE,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                policy_version=fact.policy_version,
                request_id=request_id,
                run_id=run_id,
                tenant=fact.tenant,
                account=fact.account,
            )


def use_time_fingerprint(facts: tuple[UseTimeFact, ...] | None = None) -> tuple[str, ...]:
    """Safe fingerprint bindings — no secrets or raw values."""
    items = facts if facts is not None else _pending_var.get()
    if not items:
        return ()
    out: list[str] = []
    for fact in items:
        out.append(
            ":".join(
                [
                    fact.name,
                    fact.subject_type,
                    _safe_digest(fact.subject_id),
                    fact.validator or "",
                    fact.revision or "",
                    fact.policy_version or "",
                    fact.tenant or "",
                    fact.account or "",
                    fact.require_value_digest or "",
                    fact.compare_to_arg or "",
                ]
            )
        )
    return tuple(out)


def _raise_currency(
    *,
    tool: str | None,
    fact_name: str | None,
    reason: str,
    phase: str,
    subject_ref: str | None = None,
    decide_revision: str | None = None,
    use_revision: str | None = None,
    observed_at: datetime | None = None,
    max_age_seconds: float | None = None,
    validator: str | None = None,
    policy_version: str | None = None,
    provider_precondition: str | None = None,
    provider_precondition_present: bool | None = None,
    request_id: str | None = None,
    run_id: str | None = None,
    tenant: str | None = None,
    account: str | None = None,
) -> None:
    decision = UseTimeValidation(
        decision=DECISION_DENIED,
        reason=reason,
        phase=phase,
        fact_name=fact_name,
        tool=tool,
        subject_ref=subject_ref,
        tenant=tenant,
        account=account,
        decide_revision=decide_revision,
        use_revision=use_revision,
        observed_at=observed_at,
        max_age_seconds=max_age_seconds,
        validator=validator,
        policy_version=policy_version,
        provider_precondition=provider_precondition,
        provider_precondition_present=provider_precondition_present,
        request_id=request_id,
        run_id=run_id,
    )
    _append_decision(decision)
    raise UseTimeCurrencyError(
        f"use-time currency {reason} for tool {(tool or 'tool')!r} "
        f"fact={(fact_name or 'fact')!r}",
        tool=tool,
        fact_name=fact_name,
        reason=reason,
        phase=phase,
        subject_ref=subject_ref,
    )


def _invoke_validator_sync(
    name: str,
    *,
    fact: UseTimeFact,
    kwargs: Mapping[str, Any],
    phase: str,
) -> ValidatorResult:
    fn = _validators.get(name)
    if fn is None:
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_VALIDATOR_MISSING,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
    if inspect.iscoroutinefunction(fn):
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_VALIDATOR_FAILED,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
        raise AssertionError("unreachable")  # pragma: no cover
    try:
        result = fn(
            fact=fact,
            subject_type=fact.subject_type,
            subject_id=fact.subject_id,
            kwargs=dict(kwargs),
            phase=phase,
        )
    except UseTimeCurrencyError:
        raise
    except Exception:
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_VALIDATOR_FAILED,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
        raise AssertionError("unreachable")  # pragma: no cover
    if inspect.isawaitable(result):
        # Async validator must not silently fall back on the sync path.
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_VALIDATOR_FAILED,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
    if not isinstance(result, ValidatorResult):
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_MALFORMED,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
    return result


async def _invoke_validator_async(
    name: str,
    *,
    fact: UseTimeFact,
    kwargs: Mapping[str, Any],
    phase: str,
) -> ValidatorResult:
    fn = _validators.get(name)
    if fn is None:
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_VALIDATOR_MISSING,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
        raise AssertionError("unreachable")  # pragma: no cover
    timeout = _validator_timeouts.get(name)
    try:
        if inspect.iscoroutinefunction(fn):
            coro = fn(
                fact=fact,
                subject_type=fact.subject_type,
                subject_id=fact.subject_id,
                kwargs=dict(kwargs),
                phase=phase,
            )
            if timeout is not None:
                result = await asyncio.wait_for(coro, timeout=timeout)
            else:
                result = await coro
        else:
            # Sync validators may run on async path; never the reverse.
            result = fn(
                fact=fact,
                subject_type=fact.subject_type,
                subject_id=fact.subject_id,
                kwargs=dict(kwargs),
                phase=phase,
            )
            if inspect.isawaitable(result):
                if timeout is not None:
                    result = await asyncio.wait_for(result, timeout=timeout)
                else:
                    result = await result
    except TimeoutError:
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_VALIDATOR_TIMEOUT,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
        raise AssertionError("unreachable")  # pragma: no cover
    except UseTimeCurrencyError:
        raise
    except Exception:
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_VALIDATOR_FAILED,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
        raise AssertionError("unreachable")  # pragma: no cover
    if not isinstance(result, ValidatorResult):
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_MALFORMED,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            validator=name,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            max_age_seconds=fact.max_age_seconds,
            provider_precondition=fact.provider_precondition,
        )
    return result


def _check_age(fact: UseTimeFact, *, now: datetime, phase: str) -> None:
    if fact.max_age_seconds is None:
        return
    observed = ensure_aware_utc(fact.observed_at, field="observed_at")
    age = (now - observed).total_seconds()
    # Same spirit as authority: now >= expires_at → invalid; age >= max_age → stale.
    if age >= float(fact.max_age_seconds):
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_STALE,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            observed_at=observed,
            max_age_seconds=fact.max_age_seconds,
            validator=fact.validator,
            policy_version=fact.policy_version,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
            provider_precondition=fact.provider_precondition,
        )


def _provider_precondition_present(
    fact: UseTimeFact, kwargs: Mapping[str, Any]
) -> bool | None:
    key = fact.provider_precondition
    if not key:
        return None
    return key in kwargs and kwargs.get(key) is not None


def _evaluate_use_result(
    fact: UseTimeFact,
    result: ValidatorResult,
    *,
    kwargs: Mapping[str, Any],
    phase: str,
    now: datetime,
) -> UseTimeValidation:
    observed = (
        ensure_aware_utc(result.observed_at, field="observed_at")
        if result.observed_at is not None
        else now
    )
    # Age uses decide-time observed_at unless the validator supplies a fresher stamp.
    age_basis = UseTimeFact(
        name=fact.name,
        subject_type=fact.subject_type,
        subject_id=fact.subject_id,
        observed_at=observed if result.observed_at is not None else fact.observed_at,
        tool=fact.tool,
        tenant=fact.tenant,
        account=fact.account,
        value_digest=fact.value_digest,
        revision=fact.revision,
        max_age_seconds=fact.max_age_seconds,
        request_id=fact.request_id,
        run_id=fact.run_id,
        thread_id=fact.thread_id,
        policy_version=fact.policy_version,
        validator=fact.validator,
        provenance=fact.provenance,
        provider_precondition=fact.provider_precondition,
        require_value_digest=fact.require_value_digest,
        compare_to_arg=fact.compare_to_arg,
        bind_request_id=fact.bind_request_id,
        bind_run_id=fact.bind_run_id,
        bind_thread_id=fact.bind_thread_id,
    )
    _check_age(age_basis, now=now, phase=phase)

    precond_present = _provider_precondition_present(fact, kwargs)

    if not result.current:
        reason = result.reason if result.reason != REASON_VALID else REASON_CONDITION_FALSE
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=reason if reason in {
                REASON_STALE,
                REASON_CHANGED,
                REASON_CONDITION_FALSE,
                REASON_REVISION_MISMATCH,
                REASON_SUBJECT_MISMATCH,
                REASON_TENANT_MISMATCH,
                REASON_ACCOUNT_MISMATCH,
                REASON_POLICY_CHANGED,
                REASON_VALIDATOR_MISSING,
                REASON_VALIDATOR_FAILED,
                REASON_VALIDATOR_TIMEOUT,
                REASON_UNVERIFIABLE,
                REASON_MISSING,
                REASON_MALFORMED,
            } else REASON_CONDITION_FALSE,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            use_revision=result.revision,
            observed_at=observed,
            max_age_seconds=fact.max_age_seconds,
            validator=fact.validator,
            policy_version=fact.policy_version,
            provider_precondition=fact.provider_precondition,
            provider_precondition_present=precond_present,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
        )

    result_digest = result.digest()
    if fact.revision is not None and result.revision is None or (
        (
            fact.value_digest is not None
            or fact.require_value_digest is not None
            or fact.compare_to_arg is not None
        )
        and result_digest is None
    ):
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_UNVERIFIABLE,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            use_revision=result.revision,
            observed_at=observed,
            max_age_seconds=fact.max_age_seconds,
            validator=fact.validator,
            policy_version=fact.policy_version,
            provider_precondition=fact.provider_precondition,
            provider_precondition_present=precond_present,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
        )

    if fact.revision is not None:
        if str(fact.revision) != str(result.revision):
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=REASON_REVISION_MISMATCH,
                phase=phase,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                use_revision=result.revision,
                observed_at=observed,
                max_age_seconds=fact.max_age_seconds,
                validator=fact.validator,
                policy_version=fact.policy_version,
                provider_precondition=fact.provider_precondition,
                provider_precondition_present=precond_present,
                request_id=fact.request_id,
                run_id=fact.run_id,
                tenant=fact.tenant,
                account=fact.account,
            )

    if fact.require_value_digest is not None:
        if result_digest != fact.require_value_digest:
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=REASON_CONDITION_FALSE,
                phase=phase,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                use_revision=result.revision,
                observed_at=observed,
                max_age_seconds=fact.max_age_seconds,
                validator=fact.validator,
                policy_version=fact.policy_version,
                provider_precondition=fact.provider_precondition,
                provider_precondition_present=precond_present,
                request_id=fact.request_id,
                run_id=fact.run_id,
                tenant=fact.tenant,
                account=fact.account,
            )

    if fact.compare_to_arg:
        arg_value = _lookup_path(kwargs, fact.compare_to_arg)
        if arg_value is _MISSING:
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=REASON_MISSING,
                phase=phase,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                use_revision=result.revision,
                observed_at=observed,
                max_age_seconds=fact.max_age_seconds,
                validator=fact.validator,
                policy_version=fact.policy_version,
                provider_precondition=fact.provider_precondition,
                provider_precondition_present=precond_present,
                request_id=fact.request_id,
                run_id=fact.run_id,
                tenant=fact.tenant,
                account=fact.account,
            )
        if result_digest is not None and value_digest(arg_value) != result_digest:
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=REASON_CHANGED,
                phase=phase,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                use_revision=result.revision,
                observed_at=observed,
                max_age_seconds=fact.max_age_seconds,
                validator=fact.validator,
                policy_version=fact.policy_version,
                provider_precondition=fact.provider_precondition,
                provider_precondition_present=precond_present,
                request_id=fact.request_id,
                run_id=fact.run_id,
                tenant=fact.tenant,
                account=fact.account,
            )
        elif fact.value_digest is not None and value_digest(arg_value) != fact.value_digest:
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=REASON_CHANGED,
                phase=phase,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                use_revision=result.revision,
                observed_at=observed,
                max_age_seconds=fact.max_age_seconds,
                validator=fact.validator,
                policy_version=fact.policy_version,
                provider_precondition=fact.provider_precondition,
                provider_precondition_present=precond_present,
                request_id=fact.request_id,
                run_id=fact.run_id,
                tenant=fact.tenant,
                account=fact.account,
            )

    if (
        fact.value_digest is not None
        and result_digest is not None
        and fact.require_value_digest is None
        and not fact.compare_to_arg
        and result_digest != fact.value_digest
    ):
        _raise_currency(
            tool=fact.tool,
            fact_name=fact.name,
            reason=REASON_CHANGED,
            phase=phase,
            subject_ref=fact.subject_ref,
            decide_revision=fact.revision,
            use_revision=result.revision,
            observed_at=observed,
            max_age_seconds=fact.max_age_seconds,
            validator=fact.validator,
            policy_version=fact.policy_version,
            provider_precondition=fact.provider_precondition,
            provider_precondition_present=precond_present,
            request_id=fact.request_id,
            run_id=fact.run_id,
            tenant=fact.tenant,
            account=fact.account,
        )

    return _append_decision(
        UseTimeValidation(
            decision=DECISION_ALLOWED,
            reason=REASON_VALID,
            phase=phase,
            fact_name=fact.name,
            tool=fact.tool,
            subject_ref=fact.subject_ref,
            tenant=fact.tenant,
            account=fact.account,
            decide_revision=fact.revision,
            use_revision=result.revision,
            observed_at=observed,
            max_age_seconds=fact.max_age_seconds,
            validator=fact.validator,
            policy_version=fact.policy_version,
            provider_precondition=fact.provider_precondition,
            provider_precondition_present=precond_present,
            request_id=fact.request_id,
            run_id=fact.run_id,
        )
    )


def register_fact_for_use(fact: UseTimeFact) -> UseTimeFact:
    """Bind a host fact so the use-phase check can find it later."""
    ensure_aware_utc(fact.observed_at, field="observed_at")
    current = _pending_var.get()
    kept = tuple(
        item
        for item in current
        if _pending_fact_identity(item) != _pending_fact_identity(fact)
    )
    _pending_var.set((*kept, fact))
    return fact


def authorize_use_time_facts(
    tool: str,
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
    *,
    policy: UseTimeCurrencyPolicy | None = None,
    func: Callable[..., Any] | None = None,
) -> tuple[UseTimeFact, ...]:
    """AUTHORIZE: bind decide-time facts for later USE. Does not call async validators."""
    active = policy if policy is not None else _policy_var.get()
    if active is None or not active.enabled:
        decision = UseTimeValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_DISABLED if active is not None else REASON_TIMELESS,
            phase=PHASE_AUTHORIZE,
            tool=tool,
            observed_at=use_time_now(),
        )
        _append_decision(decision)
        return ()

    tool_policy = active.tools.get(tool)
    if tool_policy is None or not tool_policy.facts:
        decision = UseTimeValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_TIMELESS,
            phase=PHASE_AUTHORIZE,
            tool=tool,
            observed_at=use_time_now(),
        )
        _append_decision(decision)
        return ()

    mapping = _bound_mapping(func, args, kwargs)
    request_id, run_id, thread_id = _call_ids(kwargs)
    captured = _captured_var.get()
    bound: list[UseTimeFact] = []

    for spec in tool_policy.facts:
        subject_id = _lookup_path(mapping, spec.id_from)
        if subject_id is _MISSING or subject_id is None or subject_id == "":
            if active.missing_policy == MISSING_POLICY_WARN:
                _append_decision(
                    UseTimeValidation(
                        decision=DECISION_SKIPPED,
                        reason=REASON_MISSING,
                        phase=PHASE_AUTHORIZE,
                        fact_name=spec.name,
                        tool=tool,
                        observed_at=use_time_now(),
                    )
                )
                continue
            _raise_currency(
                tool=tool,
                fact_name=spec.name,
                reason=REASON_MISSING,
                phase=PHASE_AUTHORIZE,
                policy_version=active.policy_version,
                request_id=request_id,
                run_id=run_id,
            )

        tenant = None
        if spec.tenant_from:
            raw_tenant = _lookup_path(mapping, spec.tenant_from)
            if raw_tenant is _MISSING or raw_tenant is None or raw_tenant == "":
                _raise_currency(
                    tool=tool,
                    fact_name=spec.name,
                    reason=REASON_MISSING,
                    phase=PHASE_AUTHORIZE,
                    policy_version=active.policy_version,
                    request_id=request_id,
                    run_id=run_id,
                )
            tenant = str(raw_tenant)
        account = None
        if spec.account_from:
            raw_account = _lookup_path(mapping, spec.account_from)
            if raw_account is _MISSING or raw_account is None or raw_account == "":
                _raise_currency(
                    tool=tool,
                    fact_name=spec.name,
                    reason=REASON_MISSING,
                    phase=PHASE_AUTHORIZE,
                    policy_version=active.policy_version,
                    request_id=request_id,
                    run_id=run_id,
                    tenant=tenant,
                )
            account = str(raw_account)

        candidates = tuple(
            item
            for item in captured
            if item.name == spec.name
            and item.subject_type == spec.subject_type
            and str(item.subject_id) == str(subject_id)
        )
        match = next(
            (
                item
                for item in candidates
                if (not spec.tenant_from or item.tenant == tenant)
                and (not spec.account_from or item.account == account)
            ),
            candidates[0] if candidates else None,
        )

        revision = None
        if spec.revision_from:
            raw_rev = _lookup_path(mapping, spec.revision_from)
            if raw_rev is not _MISSING and raw_rev is not None:
                revision = str(raw_rev)

        require_digest = None
        if spec.require is not None and "value" in spec.require:
            require_digest = value_digest(spec.require["value"])

        if match is not None:
            if spec.tenant_from and match.tenant != tenant:
                _raise_currency(
                    tool=tool,
                    fact_name=spec.name,
                    reason=REASON_TENANT_MISMATCH,
                    phase=PHASE_AUTHORIZE,
                    subject_ref=match.subject_ref,
                    decide_revision=match.revision,
                    policy_version=active.policy_version,
                    request_id=request_id,
                    run_id=run_id,
                    tenant=tenant,
                )
            if spec.account_from and match.account != account:
                _raise_currency(
                    tool=tool,
                    fact_name=spec.name,
                    reason=REASON_ACCOUNT_MISMATCH,
                    phase=PHASE_AUTHORIZE,
                    subject_ref=match.subject_ref,
                    decide_revision=match.revision,
                    policy_version=active.policy_version,
                    request_id=request_id,
                    run_id=run_id,
                    tenant=tenant,
                    account=account,
                )
            if (
                match.policy_version is not None
                and active.policy_version not in (None, "unspecified")
                and match.policy_version not in (None, "unspecified")
                and match.policy_version != active.policy_version
            ):
                _raise_currency(
                    tool=tool,
                    fact_name=spec.name,
                    reason=REASON_POLICY_CHANGED,
                    phase=PHASE_AUTHORIZE,
                    subject_ref=match.subject_ref,
                    decide_revision=match.revision,
                    policy_version=match.policy_version,
                    request_id=request_id,
                    run_id=run_id,
                    tenant=tenant,
                )
            _enforce_context_bindings(
                spec,
                match,
                tool=tool,
                request_id=request_id,
                run_id=run_id,
                thread_id=thread_id,
                policy_version=active.policy_version,
            )
            fact = UseTimeFact(
                name=match.name,
                subject_type=match.subject_type,
                subject_id=str(subject_id),
                observed_at=match.observed_at,
                tool=tool,
                tenant=tenant or match.tenant,
                account=account or match.account,
                value_digest=match.value_digest,
                revision=revision or match.revision,
                max_age_seconds=(
                    spec.max_age_seconds
                    if spec.max_age_seconds is not None
                    else match.max_age_seconds
                ),
                request_id=match.request_id,
                run_id=match.run_id,
                thread_id=match.thread_id,
                policy_version=active.policy_version,
                validator=spec.validator,
                provenance=match.provenance or "host_capture",
                provider_precondition=spec.provider_precondition or match.provider_precondition,
                require_value_digest=require_digest or match.require_value_digest,
                compare_to_arg=spec.compare_to_arg or match.compare_to_arg,
                bind_request_id=spec.bind_request_id,
                bind_run_id=spec.bind_run_id,
                bind_thread_id=spec.bind_thread_id,
            )
        else:
            # Bind from kwargs / require without calling validators at authorize.
            # Host should capture via use_time_facts.capture when decide-time
            # value must come from an authoritative source.
            if (
                active.missing_policy == MISSING_POLICY_WARN
                and require_digest is None
                and revision is None
            ):
                _append_decision(
                    UseTimeValidation(
                        decision=DECISION_SKIPPED,
                        reason=REASON_MISSING,
                        phase=PHASE_AUTHORIZE,
                        fact_name=spec.name,
                        tool=tool,
                        observed_at=use_time_now(),
                    )
                )
                continue
            if require_digest is None and revision is None and spec.compare_to_arg is None:
                # Still bind a skeleton so USE can revalidate; missing capture
                # with error policy fails closed only when no comparison basis.
                pass
            compare_value = None
            if spec.compare_to_arg:
                compare_value = _lookup_path(mapping, spec.compare_to_arg)
                if compare_value is _MISSING:
                    if active.missing_policy == MISSING_POLICY_WARN:
                        continue
                    _raise_currency(
                        tool=tool,
                        fact_name=spec.name,
                        reason=REASON_MISSING,
                        phase=PHASE_AUTHORIZE,
                        policy_version=active.policy_version,
                        request_id=request_id,
                        run_id=run_id,
                    )
            fact = UseTimeFact(
                name=spec.name,
                subject_type=spec.subject_type,
                subject_id=str(subject_id),
                observed_at=use_time_now(),
                tool=tool,
                tenant=tenant,
                account=account,
                value_digest=(
                    value_digest(compare_value) if compare_value is not None else None
                ),
                revision=revision,
                max_age_seconds=spec.max_age_seconds,
                request_id=request_id if spec.bind_request_id else None,
                run_id=run_id if spec.bind_run_id else None,
                thread_id=thread_id if spec.bind_thread_id else None,
                policy_version=active.policy_version,
                validator=spec.validator,
                provenance="kwargs_bind",
                provider_precondition=spec.provider_precondition,
                require_value_digest=require_digest,
                compare_to_arg=spec.compare_to_arg,
                bind_request_id=spec.bind_request_id,
                bind_run_id=spec.bind_run_id,
                bind_thread_id=spec.bind_thread_id,
            )

        _check_age(fact, now=use_time_now(), phase=PHASE_AUTHORIZE)
        register_fact_for_use(fact)
        bound.append(fact)
        _append_decision(
            UseTimeValidation(
                decision=DECISION_ALLOWED,
                reason=REASON_VALID,
                phase=PHASE_AUTHORIZE,
                fact_name=fact.name,
                tool=tool,
                subject_ref=fact.subject_ref,
                tenant=fact.tenant,
                account=fact.account,
                decide_revision=fact.revision,
                observed_at=fact.observed_at,
                max_age_seconds=fact.max_age_seconds,
                validator=fact.validator,
                policy_version=fact.policy_version,
                provider_precondition=fact.provider_precondition,
                provider_precondition_present=_provider_precondition_present(fact, mapping),
                request_id=fact.request_id,
                run_id=fact.run_id,
            )
        )

    return tuple(bound)


def enforce_pending_use_time_facts_at_use(
    *,
    now: datetime | None = None,
    kwargs: Mapping[str, Any] | None = None,
) -> UseTimeValidation:
    """USE: revalidate every registered fact immediately before the side effect."""
    pending = _pending_var.get()
    observed = use_time_now() if now is None else ensure_aware_utc(now, field="now")
    if not pending:
        decision = UseTimeValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_TIMELESS,
            phase=PHASE_USE,
            observed_at=observed,
        )
        return _append_decision(decision)

    policy = _policy_var.get()
    if policy is not None and not policy.enabled:
        decision = UseTimeValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_DISABLED,
            phase=PHASE_USE,
            observed_at=observed,
        )
        return _append_decision(decision)

    call_kwargs = dict(kwargs or {})
    last: UseTimeValidation | None = None
    for fact in pending:
        _enforce_context_bindings_at_use(fact, call_kwargs)
        if not fact.validator:
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=REASON_VALIDATOR_MISSING,
                phase=PHASE_USE,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                policy_version=fact.policy_version,
                request_id=fact.request_id,
                run_id=fact.run_id,
                tenant=fact.tenant,
                account=fact.account,
                max_age_seconds=fact.max_age_seconds,
            )
        result = _invoke_validator_sync(
            fact.validator,
            fact=fact,
            kwargs=call_kwargs,
            phase=PHASE_USE,
        )
        last = _evaluate_use_result(
            fact, result, kwargs=call_kwargs, phase=PHASE_USE, now=observed
        )
    assert last is not None
    return last


async def enforce_pending_use_time_facts_at_use_async(
    *,
    now: datetime | None = None,
    kwargs: Mapping[str, Any] | None = None,
) -> UseTimeValidation:
    pending = _pending_var.get()
    observed = use_time_now() if now is None else ensure_aware_utc(now, field="now")
    if not pending:
        decision = UseTimeValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_TIMELESS,
            phase=PHASE_USE,
            observed_at=observed,
        )
        return _append_decision(decision)

    policy = _policy_var.get()
    if policy is not None and not policy.enabled:
        decision = UseTimeValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_DISABLED,
            phase=PHASE_USE,
            observed_at=observed,
        )
        return _append_decision(decision)

    call_kwargs = dict(kwargs or {})
    last: UseTimeValidation | None = None
    for fact in pending:
        _enforce_context_bindings_at_use(fact, call_kwargs)
        if not fact.validator:
            _raise_currency(
                tool=fact.tool,
                fact_name=fact.name,
                reason=REASON_VALIDATOR_MISSING,
                phase=PHASE_USE,
                subject_ref=fact.subject_ref,
                decide_revision=fact.revision,
                policy_version=fact.policy_version,
                request_id=fact.request_id,
                run_id=fact.run_id,
                tenant=fact.tenant,
                account=fact.account,
                max_age_seconds=fact.max_age_seconds,
            )
        result = await _invoke_validator_async(
            fact.validator,
            fact=fact,
            kwargs=call_kwargs,
            phase=PHASE_USE,
        )
        last = _evaluate_use_result(
            fact, result, kwargs=call_kwargs, phase=PHASE_USE, now=observed
        )
    assert last is not None
    return last


def enforce_use_boundary(
    *,
    kwargs: Mapping[str, Any] | None = None,
) -> tuple[Any, UseTimeValidation]:
    """Ordered use-boundary pipeline: authority expiry, then use-time currency."""
    auth = enforce_pending_authorities_at_use()
    currency = enforce_pending_use_time_facts_at_use(kwargs=kwargs)
    return auth, currency


async def enforce_use_boundary_async(
    *,
    kwargs: Mapping[str, Any] | None = None,
) -> tuple[Any, UseTimeValidation]:
    auth = enforce_pending_authorities_at_use()
    currency = await enforce_pending_use_time_facts_at_use_async(kwargs=kwargs)
    return auth, currency


def use_time_currency_policy_for_tool(
    policy: UseTimeCurrencyPolicy, tool: str
) -> UseTimeCurrencyPolicy:
    tool_policy = policy.tools.get(tool)
    if tool_policy is None:
        return UseTimeCurrencyPolicy(
            enabled=policy.enabled,
            missing_policy=policy.missing_policy,
            policy_version=policy.policy_version,
            tools={},
        )
    return UseTimeCurrencyPolicy(
        enabled=policy.enabled,
        missing_policy=policy.missing_policy,
        policy_version=policy.policy_version,
        tools={tool: tool_policy},
    )


def apply_use_time_currency(
    func: Callable[P, R],
    policy: UseTimeCurrencyPolicy,
    *,
    tool_name: str | None = None,
    outcome_emitter: Any | None = None,
) -> Callable[P, R]:
    """Wrap *func* so facts are authorized before claim and used before body."""
    name = tool_name or getattr(func, "__name__", "tool")

    def _emit(decision: UseTimeValidation) -> None:
        if outcome_emitter is None:
            return
        try:
            outcome_emitter.emit_event(
                tool=decision.tool or name,
                request_id=decision.request_id or "",
                event="use_time_currency",
                gate=decision.decision,
                resolution_reason=decision.reason,
                run_id=decision.run_id,
                policy_version=decision.policy_version,
                tool_body_executed=False,
            )
        except Exception:
            return

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            token = set_use_time_currency_policy(policy)
            pending_token = _pending_var.set(())
            try:
                call_mapping = _bound_mapping(func, args, kwargs)
                bound = authorize_use_time_facts(
                    name, args, kwargs, policy=policy, func=func
                )
                for fact in bound:
                    _emit(
                        UseTimeValidation(
                            decision=DECISION_ALLOWED,
                            reason=REASON_VALID,
                            phase=PHASE_AUTHORIZE,
                            fact_name=fact.name,
                            tool=name,
                            subject_ref=fact.subject_ref,
                            policy_version=fact.policy_version,
                            request_id=fact.request_id,
                            run_id=fact.run_id,
                        )
                    )
                if not getattr(func, "_mycelium_ledger", False):
                    _, use_decision = await enforce_use_boundary_async(
                        kwargs=call_mapping
                    )
                    if use_decision.decision != DECISION_SKIPPED:
                        _emit(use_decision)
                return await func(*args, **kwargs)
            except UseTimeCurrencyError as exc:
                current = get_use_time_decisions()
                if current:
                    _emit(current[-1])
                else:
                    _emit(
                        UseTimeValidation(
                            decision=DECISION_DENIED,
                            reason=exc.reason,
                            phase=exc.phase,
                            fact_name=exc.fact_name,
                            tool=name,
                            subject_ref=exc.subject_ref,
                            policy_version=policy.policy_version,
                        )
                    )
                raise
            finally:
                _pending_var.reset(pending_token)
                reset_use_time_currency_policy(token)

        async_wrapper._mycelium_use_time_currency = True  # type: ignore[attr-defined]
        return async_wrapper  # type: ignore[return-value]

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        token = set_use_time_currency_policy(policy)
        pending_token = _pending_var.set(())
        try:
            call_mapping = _bound_mapping(func, args, kwargs)
            bound = authorize_use_time_facts(
                name, args, kwargs, policy=policy, func=func
            )
            for fact in bound:
                _emit(
                    UseTimeValidation(
                        decision=DECISION_ALLOWED,
                        reason=REASON_VALID,
                        phase=PHASE_AUTHORIZE,
                        fact_name=fact.name,
                        tool=name,
                        subject_ref=fact.subject_ref,
                        policy_version=fact.policy_version,
                        request_id=fact.request_id,
                        run_id=fact.run_id,
                    )
                )
            if not getattr(func, "_mycelium_ledger", False):
                _, use_decision = enforce_use_boundary(kwargs=call_mapping)
                if use_decision.decision != DECISION_SKIPPED:
                    _emit(use_decision)
            return func(*args, **kwargs)
        except UseTimeCurrencyError as exc:
            current = get_use_time_decisions()
            if current:
                _emit(current[-1])
            else:
                _emit(
                    UseTimeValidation(
                        decision=DECISION_DENIED,
                        reason=exc.reason,
                        phase=exc.phase,
                        fact_name=exc.fact_name,
                        tool=name,
                        subject_ref=exc.subject_ref,
                        policy_version=policy.policy_version,
                    )
                )
            raise
        finally:
            _pending_var.reset(pending_token)
            reset_use_time_currency_policy(token)

    sync_wrapper._mycelium_use_time_currency = True  # type: ignore[attr-defined]
    return sync_wrapper  # type: ignore[return-value]


__all__ = [
    "DECISION_ALLOWED",
    "DECISION_DENIED",
    "DECISION_SKIPPED",
    "MISSING_POLICIES",
    "MISSING_POLICY_ERROR",
    "MISSING_POLICY_WARN",
    "REASON_CHANGED",
    "REASON_ACCOUNT_MISMATCH",
    "REASON_CONDITION_FALSE",
    "REASON_MALFORMED",
    "REASON_MISSING",
    "REASON_POLICY_CHANGED",
    "REASON_REVISION_MISMATCH",
    "REASON_STALE",
    "REASON_SUBJECT_MISMATCH",
    "REASON_TENANT_MISMATCH",
    "REASON_UNVERIFIABLE",
    "REASON_VALID",
    "REASON_VALIDATOR_FAILED",
    "REASON_VALIDATOR_MISSING",
    "REASON_VALIDATOR_TIMEOUT",
    "AuthorityValidationPhase",
    "UseTimeCurrencyError",
    "UseTimeCurrencyPolicy",
    "UseTimeFact",
    "UseTimeFactSpec",
    "UseTimeToolPolicy",
    "UseTimeValidation",
    "ValidatorResult",
    "apply_use_time_currency",
    "authorize_use_time_facts",
    "clear_captured_use_time_facts",
    "clear_pending_use_time_facts",
    "enforce_pending_use_time_facts_at_use",
    "enforce_pending_use_time_facts_at_use_async",
    "enforce_use_boundary",
    "enforce_use_boundary_async",
    "get_captured_use_time_facts",
    "get_pending_use_time_facts",
    "get_use_time_currency_policy",
    "get_use_time_decisions",
    "register_fact_for_use",
    "register_use_time_validator",
    "registered_use_time_validators",
    "reset_use_time_clock",
    "reset_use_time_currency_policy",
    "reset_use_time_currency_state",
    "set_use_time_clock",
    "set_use_time_currency_policy",
    "subject_ref",
    "use_time_currency_policy_for_tool",
    "use_time_facts",
    "use_time_fingerprint",
    "use_time_now",
    "value_digest",
]
