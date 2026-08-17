"""Authority-window expiry: re-validate time-bounded authority at use.

Authorization earlier in the run is not enough. Consequential operations
must not cross a side-effect boundary on authority that expired after
authorization but before use.

This module is the shared primitive for destructive-confirm (AF-011) and
for batch item 5 (use-time currency). It does **not** complete use-time
currency by itself — policy/state freshness beyond expiry lands in item 5.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from mycelium._compat import StrEnum
from mycelium.tool_boundary import ToolBoundaryError

USE_TIME_CHECK_REQUIRED = "required"
USE_TIME_CHECK_OPTIONAL = "optional"
USE_TIME_CHECKS = frozenset({USE_TIME_CHECK_REQUIRED, USE_TIME_CHECK_OPTIONAL})

PHASE_AUTHORIZE = "authorize"
PHASE_USE = "use"


class AuthorityValidationPhase(StrEnum):
    """When an authority check ran."""

    AUTHORIZE = PHASE_AUTHORIZE
    USE = PHASE_USE


DECISION_ALLOWED = "allowed"
DECISION_EXPIRED = "expired"
DECISION_DENIED = "denied"
DECISION_SKIPPED = "skipped"

REASON_VALID = "valid"
REASON_EXPIRED = "expired"
REASON_POLICY_MISMATCH = "policy_mismatch"
REASON_TIMELESS = "timeless"
REASON_DISABLED = "disabled"


class AuthorityExpiredError(ToolBoundaryError):
    """Authority was valid at authorize but expired before use."""

    def __init__(
        self,
        message: str,
        *,
        tool: str | None = None,
        authority_ref: str | None = None,
        expires_at: datetime | None = None,
        observed_at: datetime | None = None,
        phase: str = PHASE_USE,
        operation: str | None = None,
        object_ref: str | None = None,
    ) -> None:
        super().__init__(
            message,
            violation="authority_window",
            tool_name=tool or "tool",
            llm_message=(
                f"Authority expired before use for {(tool or 'tool')!r}. "
                "Re-authorize with a fresh host grant. The tool body was not "
                "executed and the side-effect boundary was not crossed."
            ),
            field=phase,
            expected="valid_authority",
            recovery_hint=(
                "Issue a new host authorization with a future expires_at. "
                "Mycelium does not renew, extend, or substitute expired authority."
            ),
        )
        self.tool = tool
        self.authority_ref = authority_ref
        self.expires_at = expires_at
        self.observed_at = observed_at
        self.phase = phase
        self.operation = operation
        self.object_ref = object_ref


@dataclass(frozen=True)
class AuthorityValidation:
    """Payload-free result of an authorize- or use-phase check."""

    decision: str
    reason: str
    phase: str
    authority_ref: str | None = None
    authority_kind: str | None = None
    tool: str | None = None
    operation: str | None = None
    object_ref: str | None = None
    policy_version: str | None = None
    expires_at: datetime | None = None
    observed_at: datetime | None = None
    request_id: str | None = None
    run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reason": self.reason,
            "phase": self.phase,
            "authority_ref": self.authority_ref,
            "authority_kind": self.authority_kind,
            "tool": self.tool,
            "operation": self.operation,
            "object_ref": self.object_ref,
            "policy_version": self.policy_version,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "request_id": self.request_id,
            "run_id": self.run_id,
        }


@dataclass(frozen=True)
class BoundAuthority:
    """Time-bounded authority registered for a mandatory use-phase check.

    Timeless authority is never registered here. Only host/infrastructure
    clocks supply ``now`` — never model-provided time.
    """

    authority_id: str
    authority_kind: str
    expires_at: datetime
    tool: str
    operation: str | None = None
    object_ref: str | None = None
    policy_version: str | None = None
    request_id: str | None = None
    run_id: str | None = None
    issued_at: datetime | None = None

    def __post_init__(self) -> None:
        ensure_aware_utc(self.expires_at, field="expires_at")
        if self.issued_at is not None:
            ensure_aware_utc(self.issued_at, field="issued_at")

    @property
    def authority_ref(self) -> str:
        return authority_digest(self.authority_id)


@dataclass(frozen=True)
class AuthorityWindowPolicy:
    """Host-owned authority-window configuration."""

    enabled: bool = True
    use_time_check: str = USE_TIME_CHECK_REQUIRED
    clock_skew_tolerance_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.use_time_check not in USE_TIME_CHECKS:
            raise ValueError(
                "authority_window.use_time_check must be one of "
                f"{sorted(USE_TIME_CHECKS)}, got {self.use_time_check!r}"
            )
        if self.clock_skew_tolerance_seconds < 0:
            raise ValueError(
                "authority_window.clock_skew_tolerance_seconds must be >= 0"
            )


_clock_var: ContextVar[Callable[[], float] | None] = ContextVar(
    "mycelium_authority_clock", default=None
)
_policy_var: ContextVar[AuthorityWindowPolicy | None] = ContextVar(
    "mycelium_authority_window_policy", default=None
)
_pending_var: ContextVar[tuple[BoundAuthority, ...]] = ContextVar(
    "mycelium_pending_authorities", default=()
)
_decision_var: ContextVar[tuple[AuthorityValidation, ...]] = ContextVar(
    "mycelium_authority_decisions", default=()
)
_adapters: dict[str, Callable[..., AuthorityValidation]] = {}


def authority_digest(authority_id: str) -> str:
    return hashlib.sha256(authority_id.encode()).hexdigest()[:16]


def ensure_aware_utc(value: datetime, *, field: str) -> datetime:
    """Require a timezone-aware UTC instant. Reject naive timestamps."""
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime")
    if value.tzinfo is None:
        raise ValueError(
            f"{field} must be timezone-aware UTC (naive timestamps are rejected)"
        )
    return value.astimezone(timezone.utc)


def as_utc_datetime(value: Any, *, field: str = "expires_at") -> datetime:
    """Parse a trusted host expiry into aware UTC.

    Accepts aware ``datetime`` or a numeric UTC epoch seconds. Rejects naive
    datetimes, invalid values, and non-finite numbers.
    """
    if isinstance(value, datetime):
        return ensure_aware_utc(value, field=field)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            ts = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} is not a valid timestamp") from exc
        if ts != ts or ts in (float("inf"), float("-inf")):  # noqa: PLR0124
            raise ValueError(f"{field} is not a finite timestamp")
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    raise ValueError(f"{field} must be an aware datetime or UTC epoch seconds")


def utc_now() -> datetime:
    """Host/infrastructure clock. Never accepts model-provided time."""
    clock = _clock_var.get()
    if clock is not None:
        return datetime.fromtimestamp(float(clock()), tz=timezone.utc)
    return datetime.now(timezone.utc)


def set_authority_clock(
    clock: Callable[[], float] | None,
) -> Token[Callable[[], float] | None]:
    """Install an injectable clock for host setup and tests only."""
    return _clock_var.set(clock)


def reset_authority_clock(token: Token[Callable[[], float] | None]) -> None:
    _clock_var.reset(token)


def get_authority_window_policy() -> AuthorityWindowPolicy | None:
    return _policy_var.get()


def set_authority_window_policy(
    policy: AuthorityWindowPolicy | None,
) -> Token[AuthorityWindowPolicy | None]:
    return _policy_var.set(policy)


def reset_authority_window_policy(token: Token[AuthorityWindowPolicy | None]) -> None:
    _policy_var.reset(token)


def get_pending_authorities() -> tuple[BoundAuthority, ...]:
    return _pending_var.get()


def get_authority_decisions() -> tuple[AuthorityValidation, ...]:
    return _decision_var.get()


def reset_authority_window_state() -> None:
    _clock_var.set(None)
    _policy_var.set(None)
    _pending_var.set(())
    _decision_var.set(())
    _adapters.clear()


def register_authority_use_adapter(
    kind: str, fn: Callable[..., AuthorityValidation]
) -> None:
    """Register a host adapter that participates in use-time checks."""
    if not isinstance(kind, str) or not kind.strip():
        raise ValueError("authority adapter kind must be a non-empty string")
    _adapters[kind.strip()] = fn


def registered_authority_use_adapters() -> frozenset[str]:
    return frozenset(_adapters)


def _append_decision(decision: AuthorityValidation) -> AuthorityValidation:
    current = _decision_var.get()
    _decision_var.set((*current, decision))
    return decision


def _effective_expires_at(
    expires_at: datetime,
    *,
    skew_seconds: float,
) -> datetime:
    """Skew narrows validity only — never extends expired authority."""
    if skew_seconds <= 0:
        return expires_at
    return expires_at - timedelta(seconds=float(skew_seconds))


def validate_authority(
    authority: BoundAuthority | Mapping[str, Any] | None,
    *,
    now: datetime | None = None,
    expected_policy_version: str | None = None,
    phase: AuthorityValidationPhase | str = AuthorityValidationPhase.AUTHORIZE,
    skew_seconds: float = 0.0,
    context: Mapping[str, Any] | None = None,
) -> AuthorityValidation:
    """Validate a time-bounded authority at authorize or use.

    ``now`` may only be supplied by host/test code via the injectable clock
    or an explicit host call. Production tool paths must not pass model time.
    """
    del context  # reserved for item 5 / adapters
    phase_value = (
        phase.value if isinstance(phase, AuthorityValidationPhase) else str(phase)
    )
    if authority is None:
        decision = AuthorityValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_TIMELESS,
            phase=phase_value,
            observed_at=utc_now() if now is None else ensure_aware_utc(now, field="now"),
        )
        return _append_decision(decision)

    if isinstance(authority, Mapping):
        raise AuthorityExpiredError(
            "authority must be a host BoundAuthority, not a model-controlled mapping",
            phase=phase_value,
        )

    observed = utc_now() if now is None else ensure_aware_utc(now, field="now")
    expires = ensure_aware_utc(authority.expires_at, field="expires_at")
    effective = _effective_expires_at(expires, skew_seconds=skew_seconds)

    if (
        expected_policy_version is not None
        and authority.policy_version is not None
        and authority.policy_version != expected_policy_version
        and expected_policy_version != "unspecified"
        and authority.policy_version != "unspecified"
    ):
        decision = AuthorityValidation(
            decision=DECISION_DENIED,
            reason=REASON_POLICY_MISMATCH,
            phase=phase_value,
            authority_ref=authority.authority_ref,
            authority_kind=authority.authority_kind,
            tool=authority.tool,
            operation=authority.operation,
            object_ref=authority.object_ref,
            policy_version=authority.policy_version,
            expires_at=expires,
            observed_at=observed,
            request_id=authority.request_id,
            run_id=authority.run_id,
        )
        _append_decision(decision)
        raise AuthorityExpiredError(
            f"authority policy version mismatch for tool {authority.tool!r}",
            tool=authority.tool,
            authority_ref=authority.authority_ref,
            expires_at=expires,
            observed_at=observed,
            phase=phase_value,
            operation=authority.operation,
            object_ref=authority.object_ref,
        )

    if observed >= effective:
        decision = AuthorityValidation(
            decision=DECISION_EXPIRED,
            reason=REASON_EXPIRED,
            phase=phase_value,
            authority_ref=authority.authority_ref,
            authority_kind=authority.authority_kind,
            tool=authority.tool,
            operation=authority.operation,
            object_ref=authority.object_ref,
            policy_version=authority.policy_version,
            expires_at=expires,
            observed_at=observed,
            request_id=authority.request_id,
            run_id=authority.run_id,
        )
        _append_decision(decision)
        raise AuthorityExpiredError(
            f"authority expired before {phase_value} for tool {authority.tool!r} "
            f"(expires_at={expires.isoformat()}, observed_at={observed.isoformat()})",
            tool=authority.tool,
            authority_ref=authority.authority_ref,
            expires_at=expires,
            observed_at=observed,
            phase=phase_value,
            operation=authority.operation,
            object_ref=authority.object_ref,
        )

    decision = AuthorityValidation(
        decision=DECISION_ALLOWED,
        reason=REASON_VALID,
        phase=phase_value,
        authority_ref=authority.authority_ref,
        authority_kind=authority.authority_kind,
        tool=authority.tool,
        operation=authority.operation,
        object_ref=authority.object_ref,
        policy_version=authority.policy_version,
        expires_at=expires,
        observed_at=observed,
        request_id=authority.request_id,
        run_id=authority.run_id,
    )
    return _append_decision(decision)


def validate_authority_at_use(
    authority: BoundAuthority | Mapping[str, Any] | None = None,
    *,
    now: datetime | None = None,
    expected_policy_version: str | None = None,
    context: Mapping[str, Any] | None = None,
) -> AuthorityValidation:
    """Mandatory use-phase check immediately before the side-effect boundary."""
    policy = _policy_var.get()
    skew = float(policy.clock_skew_tolerance_seconds) if policy is not None else 0.0
    if authority is not None:
        kind = (
            authority.authority_kind
            if isinstance(authority, BoundAuthority)
            else str((authority or {}).get("authority_kind") or "")
        )
        adapter = _adapters.get(kind)
        if adapter is not None:
            return adapter(
                authority,
                now=now,
                expected_policy_version=expected_policy_version,
                context=context,
                phase=AuthorityValidationPhase.USE,
            )
        return validate_authority(
            authority,
            now=now,
            expected_policy_version=expected_policy_version,
            phase=AuthorityValidationPhase.USE,
            skew_seconds=skew,
            context=context,
        )
    return enforce_pending_authorities_at_use(
        now=now,
        expected_policy_version=expected_policy_version,
    )


def register_authority_for_use(authority: BoundAuthority) -> BoundAuthority:
    """Bind host authority so the use-phase check can find it later."""
    ensure_aware_utc(authority.expires_at, field="expires_at")
    current = _pending_var.get()
    kept = tuple(item for item in current if item.authority_id != authority.authority_id)
    _pending_var.set((*kept, authority))
    return authority


def clear_pending_authorities() -> None:
    _pending_var.set(())


def enforce_pending_authorities_at_use(
    *,
    now: datetime | None = None,
    expected_policy_version: str | None = None,
) -> AuthorityValidation:
    """Validate every registered time-bounded authority at the use boundary.

    No-op (skipped) when nothing is pending — timeless paths stay unchanged.
    """
    pending = _pending_var.get()
    if not pending:
        decision = AuthorityValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_TIMELESS,
            phase=PHASE_USE,
            observed_at=utc_now() if now is None else ensure_aware_utc(now, field="now"),
        )
        return _append_decision(decision)

    policy = _policy_var.get()
    # Destructive-confirm registers authorities even when authority_window is
    # omitted; use-time remains mandatory for those bindings.
    if (
        policy is not None
        and not policy.enabled
        and not any(item.authority_kind == "destructive_grant" for item in pending)
    ):
        decision = AuthorityValidation(
            decision=DECISION_SKIPPED,
            reason=REASON_DISABLED,
            phase=PHASE_USE,
            observed_at=utc_now() if now is None else ensure_aware_utc(now, field="now"),
        )
        return _append_decision(decision)

    last: AuthorityValidation | None = None
    for item in pending:
        last = validate_authority_at_use(
            item,
            now=now,
            expected_policy_version=expected_policy_version or item.policy_version,
        )
    assert last is not None
    return last


def bound_authority_from_destructive_grant(
    grant: Any,
    *,
    tool: str,
    object_ref: str | None = None,
    request_id: str | None = None,
    run_id: str | None = None,
) -> BoundAuthority:
    """Build a BoundAuthority from a host DestructiveGrant."""
    expires = as_utc_datetime(grant.expires_at, field="expires_at")
    issued = as_utc_datetime(grant.issued_at, field="issued_at")
    return BoundAuthority(
        authority_id=str(grant.grant_id),
        authority_kind="destructive_grant",
        expires_at=expires,
        tool=tool,
        operation=str(getattr(grant, "operation", "") or "") or None,
        object_ref=object_ref,
        policy_version=str(getattr(grant, "policy_version", "") or "") or None,
        request_id=request_id or getattr(grant, "request_id", None),
        run_id=run_id or getattr(grant, "run_id", None),
        issued_at=issued,
    )


__all__ = [
    "PHASE_AUTHORIZE",
    "PHASE_USE",
    "USE_TIME_CHECK_OPTIONAL",
    "USE_TIME_CHECK_REQUIRED",
    "USE_TIME_CHECKS",
    "AuthorityExpiredError",
    "AuthorityValidation",
    "AuthorityValidationPhase",
    "AuthorityWindowPolicy",
    "BoundAuthority",
    "as_utc_datetime",
    "authority_digest",
    "bound_authority_from_destructive_grant",
    "clear_pending_authorities",
    "enforce_pending_authorities_at_use",
    "ensure_aware_utc",
    "get_authority_decisions",
    "get_authority_window_policy",
    "get_pending_authorities",
    "register_authority_for_use",
    "register_authority_use_adapter",
    "registered_authority_use_adapters",
    "reset_authority_clock",
    "reset_authority_window_policy",
    "reset_authority_window_state",
    "set_authority_clock",
    "set_authority_window_policy",
    "utc_now",
    "validate_authority",
    "validate_authority_at_use",
]
