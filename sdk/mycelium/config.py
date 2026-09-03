"""Stable configuration API facade."""

# Compatibility facades intentionally re-export the historical import surface.
# ruff: noqa: F401

from __future__ import annotations

import functools
import importlib
import inspect
import os
import re
import warnings
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from mycelium.action_ledger import (
    ARGS_DRIFT_POLICIES,
    ARGS_DRIFT_SOFT,
    UNCLASSIFIED_POLICY_STRICT,
    UNCLASSIFIED_POLICY_WARN,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    LedgerStorage,
    ledger,
    ledger_sync,
)
from mycelium.audit_receipt import (
    AtomicAuditReceiptStorage,
    AuditReceiptEmitter,
    AuditReceiptStorage,
    FileAuditReceiptStorage,
    InMemoryAuditReceiptStorage,
    resolve_signing_key,
)
from mycelium.authority_window import (
    USE_TIME_CHECK_REQUIRED,
    USE_TIME_CHECKS,
    AuthorityWindowPolicy,
    set_authority_window_policy,
)

# Historical module-level imports retained by the compatibility facade.
from mycelium.budget_guard import (
    MISSING_USAGE_POLICIES,
    MISSING_USAGE_POLICY_ERROR,
    MISSING_USAGE_POLICY_WARN,
    ON_MISSING_HARD,
    ON_MISSING_METER_MODES,
    BudgetCeilings,
    BudgetGuard,
    BudgetGuardStorage,
    FileBudgetGuardStorage,
    InMemoryBudgetGuardStorage,
    PostgresBudgetGuardStorage,
    RedisBudgetGuardStorage,
    SqliteBudgetGuardStorage,
    apply_budget_guard,
    parse_duration_seconds,
)
from mycelium.completion_contract import (
    AtomicCompletionStorage,
    CompletionContract,
    CompletionStorage,
    FileCompletionStorage,
    InMemoryCompletionStorage,
    registered_terminal_adapters,
    set_active_completion_contract,
)
from mycelium.config_parser import (
    _AUTHORITY_WINDOW_KEYS,  # noqa: F401  compatibility re-export
    _DEPLOYMENT_TOPOLOGIES,  # noqa: F401  compatibility re-export
    _DESTRUCTIVE_GRANT_KEYS,  # noqa: F401  compatibility re-export
    _DESTRUCTIVE_OBJECT_KEYS,  # noqa: F401  compatibility re-export
    _DESTRUCTIVE_TOOL_KEYS,  # noqa: F401  compatibility re-export
    _DESTRUCTIVE_TOP_KEYS,  # noqa: F401  compatibility re-export
    _USE_TIME_FACT_KEYS,  # noqa: F401  compatibility re-export
    _USE_TIME_SUBJECT_KEYS,  # noqa: F401  compatibility re-export
    _USE_TIME_TOP_KEYS,  # noqa: F401  compatibility re-export
    _apply_action_ledger_tools,  # noqa: F401  compatibility re-export
    _apply_task_ledger_tasks,  # noqa: F401  compatibility re-export
    _enforce_memory_storage_policy,  # noqa: F401  compatibility re-export
    _enforce_production_outcome_emit,  # noqa: F401  compatibility re-export
    _memory_storage_policy,  # noqa: F401  compatibility re-export
    _missing_run_id_policy,
    _normalize_ledger_config,  # noqa: F401  compatibility re-export
    _outcome_on_failure,
    _parse_authority_window,  # noqa: F401  compatibility re-export
    _parse_completion_id_lists,
    _parse_config,
    _parse_deployment,  # noqa: F401  compatibility re-export
    _parse_destination_spec,  # noqa: F401  compatibility re-export
    _parse_destructive_confirm,  # noqa: F401  compatibility re-export
    _parse_destructive_grant,  # noqa: F401  compatibility re-export
    _parse_destructive_object,  # noqa: F401  compatibility re-export
    _parse_entity_guard,  # noqa: F401  compatibility re-export
    _parse_integrations,  # noqa: F401  compatibility re-export
    _parse_optional_non_negative_float,  # noqa: F401  compatibility re-export
    _parse_optional_positive_float,  # noqa: F401  compatibility re-export
    _parse_profile,  # noqa: F401  compatibility re-export
    _parse_secret_args,  # noqa: F401  compatibility re-export
    _parse_string_list,  # noqa: F401  compatibility re-export
    _parse_task_config,  # noqa: F401  compatibility re-export
    _parse_tool_config,  # noqa: F401  compatibility re-export
    _parse_transition_config,  # noqa: F401  compatibility re-export
    _parse_use_time_currency,  # noqa: F401  compatibility re-export
    _parse_use_time_fact,  # noqa: F401  compatibility re-export
    _parse_verify,  # noqa: F401  compatibility re-export
    _request_identity_policy,
    _scope_grant_from_config,
    _side_effecting_memory_tools,  # noqa: F401  compatibility re-export
    _validate_callable_targets,  # noqa: F401  compatibility re-export
    _validate_transition_tools,  # noqa: F401  compatibility re-export
    authority_window_policy_from_mapping,
    destructive_confirm_policy_from_mapping,
    entity_guard_policy_from_mapping,
    secret_args_policy_from_mapping,
    use_time_currency_policy_from_mapping,
)
from mycelium.config_policies import (
    _budget_ceilings_from_config,
    _missing_usage_policy,
    _parse_callable_path,
)
from mycelium.config_schema import (
    CONFIG_VERSION,
    ToolContractModel,
    config_json_schema,
)
from mycelium.config_types import (
    MEMORY_STORAGE_POLICIES,
    MEMORY_STORAGE_POLICY_ERROR,
    MEMORY_STORAGE_POLICY_WARN,
    PROFILE_DEVELOPMENT,
    PROFILE_PRODUCTION,
    PROFILES,
    AutoInstrumentationTarget,
    ConfigError,
    TaskConfig,
    ToolConfig,
)
from mycelium.contracts import apply_tool_contract, validate_contract_definition
from mycelium.decision import DecisionPolicyBundle, apply_decision_policy
from mycelium.destructive_confirm import (
    MISSING_POLICIES as DESTRUCTIVE_MISSING_POLICIES,
)
from mycelium.destructive_confirm import (
    MISSING_POLICY_ERROR as DESTRUCTIVE_MISSING_POLICY_ERROR,
)
from mycelium.destructive_confirm import (
    SHARED_GRANT_STORAGES,
    STORAGE_FILE,
    STORAGE_MEMORY,
    STORAGE_POSTGRES,
    STORAGE_REDIS,
    STORAGE_SQLITE,
    DestructiveConfirmPolicy,
    DestructiveGrantSpec,
    DestructiveObjectSpec,
    DestructiveToolPolicy,
    FileDestructiveGrantStore,
    InMemoryDestructiveGrantStore,
    PostgresDestructiveGrantStore,
    RedisDestructiveGrantStore,
    SqliteDestructiveGrantStore,
    apply_destructive_confirm,
    destructive_confirm_policy_for_tool,
)
from mycelium.entity_guard import (
    DEST_TYPES,
    MISSING_POLICIES,
    MISSING_POLICY_ERROR,
    DestinationAllow,
    DestinationSpec,
    EntityGuardPolicy,
    ToolDestinationPolicy,
    apply_entity_guard,
    entity_guard_policy_for_tool,
)
from mycelium.history_guard import HistoryGuard
from mycelium.integrations.crewai import (
    CrewAIIntegrationError,
    install_crewai_runtime,
    instrument_crewai_tool,
)
from mycelium.integrations.langgraph import (
    LangGraphIntegrationError,
    install_langgraph_completion_terminal,
    instrument_langgraph_tool,
)
from mycelium.loop_guard import (
    DEFAULT_CONSECUTIVE_SOFT,
    MISSING_RUN_ID_POLICIES,
    MISSING_RUN_ID_POLICY_ERROR,
    MISSING_RUN_ID_POLICY_WARN,
    AtomicLoopGuardStorage,
    FileLoopGuardStorage,
    InMemoryLoopGuardStorage,
    LoopGuard,
    LoopGuardStorage,
    apply_loop_guard,
)
from mycelium.loop_guard import (
    UNCLASSIFIED_POLICY_STRICT as LOOP_UNCLASSIFIED_STRICT,
)
from mycelium.loop_guard import (
    UNCLASSIFIED_POLICY_WARN as LOOP_UNCLASSIFIED_WARN,
)
from mycelium.message_validator import MessageValidator
from mycelium.outcome_emit import (
    OUTCOME_ON_FAILURE_ERROR,
    OUTCOME_ON_FAILURE_POLICIES,
    OUTCOME_ON_FAILURE_WARN,
    FileOutcomeStorage,
    InMemoryOutcomeStorage,
    OutcomeEmitter,
    OutcomeStorage,
)
from mycelium.outcome_export import (
    FanoutOutcomeStorage,
    OpenTelemetryOutcomeStorage,
    PrometheusOutcomeStorage,
    WebhookOutcomeStorage,
)
from mycelium.protect import protect, protect_sync
from mycelium.runtime_builder import (
    MyceliumConfig,
    _callable_with_name,
    _check_existing_config_wrapper,
    _import_callable,
    _mark_config_applied,
    _NoopRun,
    _ScopedRunContext,
    _SimpleNamespace,
)
from mycelium.scope_guard import (
    ON_VIOLATION_MODES,
    ON_VIOLATION_SOFT,
    AtomicScopeGuardStorage,
    FileScopeGuardStorage,
    InMemoryScopeGuardStorage,
    ScopeGrant,
    ScopeGuard,
    ScopeGuardStorage,
    apply_scope_guard,
)
from mycelium.secret_protection import (
    SECRET_ARGS_POLICIES,
    SecretArgsPolicy,
    apply_secret_args,
)
from mycelium.session import Session
from mycelium.state_authority import (
    ON_MISMATCH_HARD,
    ON_MISMATCH_MODES,
    StateAuthority,
    apply_state_authority,
)
from mycelium.state_flush import (
    AtomicStateFlushStorage,
    FileStateFlushStorage,
    InMemoryStateFlushStorage,
    StateFlush,
    StateFlushStorage,
    get_active_flush_run,
)
from mycelium.storage._helpers import resolve_storage_url
from mycelium.storage.atomic_state import (
    AtomicStateBackend,
    FileAtomicStateBackend,
    InMemoryAtomicStateBackend,
    PostgresAtomicStateBackend,
    RedisAtomicStateBackend,
)
from mycelium.task_ledger import (
    TaskFileLedgerStorage,
    TaskInMemoryLedgerStorage,
    TaskLedgerStorage,
    task_ledger,
    task_ledger_sync,
)
from mycelium.tool_boundary import bounded, bounded_sync
from mycelium.tool_registry import ToolRegistry
from mycelium.tool_runner import ToolRunner
from mycelium.transition import (
    CONSEQUENTIAL_SIDE_EFFECT_CLASSES,
    REQUEST_IDENTITY_POLICIES,
    REQUEST_IDENTITY_POLICY_DERIVED,
    REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    RetryPermission,
    SideEffectBoundary,
    SideEffectClass,
    Spendability,
    ToolCapability,
    ToolTransitionBinding,
    TransitionConfig,
    TransitionScope,
    execution_scope,
    parse_capability,
    parse_retry_permission,
    parse_side_effect_boundary,
    parse_side_effect_class,
    parse_spendability,
)
from mycelium.use_time_currency import (
    MISSING_POLICIES as USE_TIME_MISSING_POLICIES,
)
from mycelium.use_time_currency import (
    MISSING_POLICY_ERROR as USE_TIME_MISSING_POLICY_ERROR,
)
from mycelium.use_time_currency import (
    UseTimeCurrencyPolicy,
    UseTimeFactSpec,
    UseTimeToolPolicy,
    apply_use_time_currency,
    set_use_time_currency_policy,
    use_time_currency_policy_for_tool,
)

_CONFIG_APPLIED_MARKER = "_mycelium_config_applied"
_GUARD_MARKERS = (
    "_mycelium_ledger",
    "_mycelium_task_ledger",
    "_mycelium_bounded",
    "_mycelium_protected",
    "_mycelium_loop_guarded",
    "_mycelium_budget_guarded",
    "_mycelium_scope_guarded",
    "_mycelium_state_authority",
    "_mycelium_secret_args",
    "_mycelium_entity_guard",
    "_mycelium_langgraph_integration",
)


def load_config_from_string(text: str) -> MyceliumConfig:
    """Parse Mycelium config from a YAML string."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML: {exc}") from exc

    if data is None:
        data = {}

    return _parse_config(data)


def load_config(path: str | Path) -> MyceliumConfig:
    """Load Mycelium config from a YAML file."""
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")

    text = path.read_text(encoding="utf-8")
    return load_config_from_string(text)


def _load_config_for_preflight(path: str | Path) -> MyceliumConfig:
    """Validate config without activating application-owned runtime hooks."""
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML: {exc}") from exc
    return _parse_config(data or {}, activate_runtime=False)


__all__ = [
    "ConfigError",
    "MEMORY_STORAGE_POLICIES",
    "MEMORY_STORAGE_POLICY_ERROR",
    "MEMORY_STORAGE_POLICY_WARN",
    "PROFILE_DEVELOPMENT",
    "PROFILE_PRODUCTION",
    "PROFILES",
    "REQUEST_IDENTITY_POLICIES",
    "REQUEST_IDENTITY_POLICY_DERIVED",
    "REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT",
    "MyceliumConfig",
    "ToolConfig",
    "TransitionConfig",
    "config_json_schema",
    "load_config",
    "load_config_from_string",
]
