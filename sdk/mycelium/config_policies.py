"""Shared configuration normalization and production policy helpers."""

from __future__ import annotations

import re
from typing import Any

from mycelium.budget_guard import (
    MISSING_USAGE_POLICIES,
    MISSING_USAGE_POLICY_ERROR,
    MISSING_USAGE_POLICY_WARN,
    BudgetCeilings,
    parse_duration_seconds,
)
from mycelium.config_types import (
    PROFILE_DEVELOPMENT,
    PROFILE_PRODUCTION,
    ConfigError,
)

_CALLABLE_PATH_RE = re.compile(
    r"^(?:[A-Za-z_][A-Za-z0-9_]*\.)*[A-Za-z_][A-Za-z0-9_]*:"
    r"[A-Za-z_][A-Za-z0-9_]*$"
)

def _parse_callable_path(raw: Any, *, kind: str, name: str) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not _CALLABLE_PATH_RE.fullmatch(raw):
        raise ConfigError(f"{kind} {name!r}.callable must be 'package.module:function'")
    return raw

def _budget_ceilings_from_config(raw: dict[str, Any]) -> BudgetCeilings:
    """Parse ``max_duration`` / ``max_steps`` / ``max_tokens`` / ``max_usd``."""
    max_duration_raw = raw.get("max_duration")
    max_steps_raw = raw.get("max_steps")
    max_tokens_raw = raw.get("max_tokens")
    max_usd_raw = raw.get("max_usd")
    max_cost_raw = raw.get("max_cost_usd")
    max_duration: float | None = None
    max_steps: int | None = None
    max_tokens: int | None = None
    max_usd: float | None = None
    if max_duration_raw is not None:
        try:
            max_duration = parse_duration_seconds(max_duration_raw)
        except ValueError as exc:
            raise ConfigError(f"'budget.max_duration': {exc}") from exc
    if max_steps_raw is not None:
        if not isinstance(max_steps_raw, int) or isinstance(max_steps_raw, bool):
            raise ConfigError("'budget.max_steps' must be a positive int")
        max_steps = max_steps_raw
    if max_tokens_raw is not None:
        if not isinstance(max_tokens_raw, int) or isinstance(max_tokens_raw, bool):
            raise ConfigError("'budget.max_tokens' must be a positive int")
        max_tokens = max_tokens_raw
    parsed_usd: float | None = None
    parsed_cost: float | None = None
    if max_usd_raw is not None:
        try:
            parsed_usd = float(max_usd_raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("'budget.max_usd' must be a positive number") from exc
    if max_cost_raw is not None:
        try:
            parsed_cost = float(max_cost_raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("'budget.max_cost_usd' must be a positive number") from exc
    if parsed_usd is not None and parsed_cost is not None and parsed_usd != parsed_cost:
        raise ConfigError("'budget.max_usd' and 'budget.max_cost_usd' disagree; use one")
    max_usd = parsed_usd if parsed_usd is not None else parsed_cost
    try:
        return BudgetCeilings(
            max_duration=max_duration,
            max_steps=max_steps,
            max_tokens=max_tokens,
            max_usd=max_usd,
        )
    except ValueError as exc:
        raise ConfigError(f"budget: {exc}") from exc


def _missing_usage_policy(
    raw: dict[str, Any] | None,
    *,
    profile: str = PROFILE_DEVELOPMENT,
) -> str:
    """Return ``missing_usage_policy``, defaulting to ``warn``.

    ``profile: production`` with token/cost limits treats an omitted policy
    as ``error``. An explicit ``warn`` is rejected so production cannot be
    silently weakened.
    """
    ceilings = _budget_ceilings_from_config(raw or {})
    token_or_cost = ceilings.requires_usage_meter()
    if raw is None:
        return MISSING_USAGE_POLICY_WARN
    if "missing_usage_policy" in raw:
        value = raw["missing_usage_policy"]
        if value not in MISSING_USAGE_POLICIES:
            raise ConfigError(
                f"'budget.missing_usage_policy' must be "
                f"{MISSING_USAGE_POLICY_WARN!r} or "
                f"{MISSING_USAGE_POLICY_ERROR!r}, got {value!r}"
            )
        if profile == PROFILE_PRODUCTION and token_or_cost and value == MISSING_USAGE_POLICY_WARN:
            _reject_weaker_production_policy("budget.missing_usage_policy", str(value))
        return str(value)
    if profile == PROFILE_PRODUCTION and token_or_cost:
        return MISSING_USAGE_POLICY_ERROR
    return MISSING_USAGE_POLICY_WARN


def _storage_settings(cfg: dict[str, Any] | None) -> dict[str, Any]:
    """Strip integration-only keys from a global ledger/flush section."""
    if cfg is None:
        return {"storage": "memory"}
    return {
        key: value
        for key, value in cfg.items()
        if key not in ("tools", "tasks", "auto", "memory_storage_policy")
    }


def _merge_storage_settings(
    base: dict[str, Any] | None,
    override: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(_storage_settings(base))
    merged.update(override)
    return merged

def _reject_weaker_production_policy(field_path: str, value: str) -> None:
    raise ConfigError(
        f"profile is {PROFILE_PRODUCTION!r} but '{field_path}' is {value!r}; "
        f"production requires 'error' and will not silently weaken to 'warn'. "
        f"Remove '{field_path}' or set it to 'error'."
    )
