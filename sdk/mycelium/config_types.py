"""Stable configuration value objects and shared policy constants."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mycelium.config_schema import ToolContractModel
from mycelium.transition import (
    RetryPermission,
    SideEffectBoundary,
    SideEffectClass,
    Spendability,
    ToolCapability,
)


class ConfigError(Exception):
    """Raised when a Mycelium config file is invalid or inconsistent."""


MEMORY_STORAGE_POLICY_WARN = "warn"
MEMORY_STORAGE_POLICY_ERROR = "error"
MEMORY_STORAGE_POLICIES = frozenset({MEMORY_STORAGE_POLICY_WARN, MEMORY_STORAGE_POLICY_ERROR})

PROFILE_DEVELOPMENT = "development"
PROFILE_PRODUCTION = "production"
PROFILES = frozenset({PROFILE_DEVELOPMENT, PROFILE_PRODUCTION})

@dataclass(frozen=True)
class ToolConfig:
    """Parsed configuration for a single tool."""

    name: str
    protect: dict[str, Any] | None = None
    bounded: dict[str, Any] | None = None
    ledger: dict[str, Any] | None = None
    audit_receipt: bool = False
    side_effect_class: SideEffectClass | None = None
    retry_permission: RetryPermission | None = None
    side_effect_boundary: SideEffectBoundary | None = None
    spendability: Spendability | None = None
    capability: ToolCapability | None = None
    provider_idempotency_key_param: str | None = None
    provider_idempotency_key_ttl: float | None = None
    propagate_effect_id_as_provider_key: bool = False
    request_id_from: str | None = None
    callable_path: str | None = None
    # Per-tool loop_guard: None=inherit global, False=disable, dict=overrides
    loop_guard: dict[str, Any] | bool | None = None
    # Per-tool budget_guard: None=inherit global, False=disable
    budget_guard: bool | None = None
    # Per-tool scope_guard: None=inherit global, False=disable, dict=overrides
    scope_guard: dict[str, Any] | bool | None = None
    # Per-tool state_authority: None=inherit global, False=disable, dict=overrides
    state_authority: dict[str, Any] | bool | None = None
    # Fields that may hold secret:// references (resolved only at execution).
    secret_fields: tuple[str, ...] = ()
    # Per-tool secret_args: None=inherit global, False=disable
    secret_args: bool | None = None
    # Per-tool entity_guard: None=inherit global, False=disable
    entity_guard: bool | None = None
    # Per-tool destructive_confirm: None=inherit global, False=disable
    destructive_confirm: bool | None = None
    # Per-tool use_time_currency: None=inherit global, False=disable
    use_time_currency: bool | None = None
    contract: ToolContractModel | None = None

    def is_noop(self) -> bool:
        return (
            self.protect is None
            and self.bounded is None
            and self.ledger is None
            and not self.audit_receipt
            and self.loop_guard is None
            and self.budget_guard is None
            and self.scope_guard is None
            and self.state_authority is None
            and not self.secret_fields
            and self.secret_args is None
            and self.entity_guard is None
            and self.destructive_confirm is None
            and self.use_time_currency is None
            and self.contract is None
        )


@dataclass(frozen=True)
class TaskConfig:
    """Parsed configuration for a single task."""

    name: str
    ledger: dict[str, Any] | None = None
    audit_receipt: bool = False
    callable_path: str | None = None

    def is_noop(self) -> bool:
        return self.ledger is None and not self.audit_receipt


@dataclass(frozen=True)
class AutoInstrumentationTarget:
    """A YAML entry resolved by command-based auto-instrumentation."""

    kind: str
    name: str
    callable_path: str
# Preserve the historical qualified names used by repr/pickle/introspection.
for _compat_type in (ConfigError, ToolConfig, TaskConfig, AutoInstrumentationTarget):
    _compat_type.__module__ = "mycelium.config"
