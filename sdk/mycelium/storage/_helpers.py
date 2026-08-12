"""Shared helpers for durable ledger storage backends."""

from __future__ import annotations

import os
import time
from typing import Any, Literal, Protocol, TypeVar

ClaimOutcome = Literal["claimed", "completed", "in_flight"]

E = TypeVar("E")


class EntryProtocol(Protocol):
    request_id: str
    status: str
    lease_until: float | None

    def to_dict(self) -> dict[str, Any]: ...


def with_lease(
    entry: E,
    *,
    now: float,
    lease_ttl: float,
) -> E:
    """Return a copy of an in-flight entry with ``lease_until`` set when supported."""
    from dataclasses import replace

    from mycelium.transition import TerminalOutcome, legacy_status_from_terminal

    lease_until = now + lease_ttl if lease_ttl > 0 else None
    fields = getattr(entry, "__dataclass_fields__", {})
    if "lease_until" not in fields:
        return entry
    updates: dict[str, Any] = {"lease_until": lease_until}
    if "last_heartbeat_at" in fields:
        updates["last_heartbeat_at"] = now
    if "terminal_outcome" in fields:
        updates["terminal_outcome"] = TerminalOutcome.IN_FLIGHT.value
    if "status" in fields:
        updates["status"] = legacy_status_from_terminal(TerminalOutcome.IN_FLIGHT)
    return replace(entry, **updates)


def _entry_terminal_outcome(entry: E, *, now: float) -> str | None:
    raw = getattr(entry, "terminal_outcome", None)
    if raw is not None:
        from mycelium.transition import resolve_terminal_outcome

        return resolve_terminal_outcome(
            raw,
            lease_until=getattr(entry, "lease_until", None),
            now=now,
        ).value
    return None


def claim_inflight_outcome(
    existing: E | None,
    *,
    now: float,
) -> ClaimOutcome:
    """Classify an existing entry before attempting a new in-flight claim."""
    if existing is None:
        return "claimed"

    terminal = _entry_terminal_outcome(existing, now=now)
    if terminal is not None:
        from mycelium.transition import TerminalOutcome

        if terminal == TerminalOutcome.COMPLETED.value:
            return "completed"
        if terminal == TerminalOutcome.IN_FLIGHT.value:
            return "in_flight"
        if terminal == TerminalOutcome.EXPIRED.value:
            return "claimed"
        if terminal in (
            TerminalOutcome.FAILED_BEFORE_EFFECT.value,
            TerminalOutcome.FAILED_AFTER_EFFECT.value,
        ):
            return "claimed"
        if terminal == TerminalOutcome.BLOCKED.value:
            return "in_flight"
        if terminal == TerminalOutcome.UNKNOWN.value:
            return "in_flight"

    if existing.status == "completed":
        return "completed"
    if existing.status == "in-flight":
        lease_until = getattr(existing, "lease_until", None)
        if lease_until is not None and now >= lease_until:
            return "claimed"
        return "in_flight"
    if existing.status == "failed":
        return "claimed"
    return "claimed"


def default_try_claim_inflight(
    storage: Any,
    entry: E,
    *,
    now: float | None = None,
    lease_ttl: float = 3600.0,
) -> tuple[ClaimOutcome, E | None]:
    """Non-atomic claim used by memory storage; file storage wraps this in a lock."""
    now = now if now is not None else time.time()
    existing = storage.get(entry.request_id)
    outcome = claim_inflight_outcome(existing, now=now)
    if outcome == "completed":
        return "completed", existing
    if outcome == "in_flight":
        return "in_flight", existing
    leased = with_lease(entry, now=now, lease_ttl=lease_ttl)
    storage.set(leased)
    return "claimed", None


def lease_allows_renew(lease_until: float | None, *, now: float) -> bool:
    """True when a renew CAS may extend the lease (held or unbounded)."""
    from mycelium.transition import LeaseValidity, resolve_lease_validity

    return resolve_lease_validity(lease_until, now=now) != LeaseValidity.EXPIRED


def expand_env_value(value: str) -> str:
    """Expand ``$VAR`` / ``${VAR}`` placeholders from the process environment."""
    return os.path.expandvars(value)


def redact_secrets(text: str) -> str:
    """Redact userinfo / password material from URLs and DSNs in messages."""
    import re

    # postgresql://user:pass@host → postgresql://***@host
    # redis://:pass@host → redis://***@host
    redacted = re.sub(
        r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^/\s@]+)@",
        r"\1***@",
        text,
    )
    redacted = re.sub(
        r"(?i)\b([a-z][a-z0-9+.-]*://):([^/\s@]+)@",
        r"\1***@",
        redacted,
    )
    # password=... / passwd=... query or keyword forms
    redacted = re.sub(
        r"(?i)\b(password|passwd|pwd)\s*=\s*([^\s\"']+)",
        r"\1=***",
        redacted,
    )
    return redacted


def resolve_storage_url(
    raw: dict[str, Any],
    *,
    url_key: str = "url",
    alt_keys: tuple[str, ...] = (),
) -> str:
    """Resolve a connection string from config or an environment variable.

    Prefers ``url_key`` / ``{url_key}_env``, then each name in *alt_keys*
    (e.g. ``("dsn",)`` so Postgres configs may use either ``url`` or ``dsn``).
    Values support ``$VAR`` / ``${VAR}`` expansion.
    """
    keys = (url_key, *alt_keys)
    for key in keys:
        if key in raw and raw[key] is not None and str(raw[key]).strip():
            return expand_env_value(str(raw[key]))
        env_key = raw.get(f"{key}_env")
        if env_key:
            value = os.environ.get(str(env_key))
            if not value:
                raise ValueError(f"environment variable {env_key!r} is not set")
            return expand_env_value(value)
    labels = " or ".join(repr(k) for k in keys)
    env_labels = " or ".join(f"{k}_env" for k in keys)
    raise ValueError(f"storage requires {labels} or {env_labels}")
