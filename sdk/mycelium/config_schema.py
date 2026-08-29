"""Versioned, machine-readable model for ``mycelium.yaml``.

This module owns the structural configuration contract used by editors and
agents.  The semantic parser in :mod:`mycelium.config` remains authoritative
for cross-field safety rules and runtime object construction.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

CONFIG_VERSION = 1
CONFIG_SCHEMA_ID = "https://mycelium-labs.github.io/schema/mycelium-config-v1.json"


class _ConfigModel(BaseModel):
    """Strictly type declared fields while preserving extension compatibility."""

    model_config = ConfigDict(extra="allow", strict=True)


class StorageConfigModel(_ConfigModel):
    """Common durable-state backend settings."""

    storage: str | None = Field(
        default=None,
        json_schema_extra={
            "enum": ["memory", "file", "sqlite", "redis", "postgres", "shared", None]
        },
    )
    path: str | None = None
    table: str | None = None
    namespace: str | None = None
    prefix: str | None = None
    url: str | None = None
    url_env: str | None = None
    dsn: str | None = None
    dsn_env: str | None = None


class BudgetConfigModel(StorageConfigModel):
    """Run-wide ceilings for protected calls, time, tokens, and cost."""

    max_duration: float | str | None = Field(
        default=None,
        description="Wall-clock ceiling for the run, in seconds or with a duration suffix.",
    )
    max_steps: int | None = Field(
        default=None,
        gt=0,
        description=(
            "Run-wide protected-call ceiling. Each budget-guarded tool invocation and "
            "instrumented LLM turn reserves one step; business workflow counters are separate."
        ),
    )
    max_tokens: int | None = Field(default=None, gt=0)
    max_usd: int | float | None = Field(default=None, gt=0)
    max_cost_usd: int | float | None = Field(default=None, gt=0)
    missing_usage_policy: Literal["warn", "error"] | None = None
    warn_at: int | float | None = Field(default=None, gt=0, le=1)
    on_missing_meter: Literal["warn", "hard"] | None = None


class CompletionConfigModel(StorageConfigModel):
    """Completion storage and optional custom-runtime startup adapter."""

    adapter_installer: str | None = Field(
        default=None,
        description=(
            "Import path (package.module:function) called during runtime config "
            "activation. It must wire the custom terminal boundary and call "
            "register_terminal_adapter()."
        ),
    )


class TransitionConfigModel(_ConfigModel):
    """Stable identity and retry timing for guarded transitions."""

    agent_id: str
    policy_version: str
    scope_from: dict[str, str] = Field(default_factory=dict)
    lease_ttl: float | None = Field(default=None, gt=0)
    lease_renew_interval: float | None = Field(default=None, ge=0)
    poll_interval: float | None = Field(default=None, gt=0)
    poll_timeout: float | None = Field(default=None, gt=0)
    reclaim_requires_death_signal: bool = True
    presumed_dead_after: float | None = Field(default=None, gt=0)


class LedgerConfigModel(StorageConfigModel):
    """Defaults and allowlist for tool-level durable execution."""

    tools: Literal["all"] | list[str] | None = None
    unclassified_policy: str | None = Field(
        default=None, json_schema_extra={"enum": ["warn", "strict", None]}
    )
    memory_storage_policy: str | None = Field(
        default=None, json_schema_extra={"enum": ["warn", "error", None]}
    )
    request_identity_policy: str | None = Field(
        default=None,
        json_schema_extra={"enum": ["derived", "require_explicit", None]},
    )
    # Runtime validation intentionally remains in ActionLedger construction so
    # existing applications keep the same load-versus-use failure timing.
    on_args_drift: str | None = Field(
        default=None,
        json_schema_extra={"enum": ["soft", "hard", "off"]},
    )
    missing_run_id_policy: str | None = Field(
        default=None, json_schema_extra={"enum": ["warn", "error", None]}
    )


class TaskLedgerConfigModel(StorageConfigModel):
    """Defaults and allowlist for task-level durable execution."""

    tasks: Literal["all"] | list[str] | None = None


class ToolConfigModel(_ConfigModel):
    """Configuration for one application tool."""

    callable: str | None = Field(
        default=None,
        description="Import path in package.module:function form.",
        json_schema_extra={
            "pattern": (
                r"^(?:[A-Za-z_][A-Za-z0-9_]*\.)*"
                r"[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*$"
            )
        },
    )
    protect: dict[str, Any] | None = None
    bounded: dict[str, Any] | None = None
    ledger: bool | dict[str, Any] | None = None
    audit_receipt: bool = False
    side_effect_class: str | None = Field(
        default=None,
        json_schema_extra={
            "enum": [
                "read",
                "idempotent_mutate",
                "keyed_mutate",
                "non_idempotent_mutate",
                "irreversible",
                "read_only",
                "idempotent_write",
                "external_api_mutation",
                "non_idempotent_write",
                "payment",
                "email",
                "subagent",
                "onchain_action",
                None,
            ]
        },
    )
    retry_permission: str | None = Field(
        default=None,
        json_schema_extra={
            "enum": [
                "safe_retry",
                "retry_only_with_same_provider_idempotency_key",
                "manual_reconciliation_required",
                "never_retry_automatically",
                None,
            ]
        },
    )
    side_effect_boundary: str | None = Field(
        default=None,
        json_schema_extra={"enum": ["not_crossed", "maybe_crossed", "crossed", None]},
    )
    spendability: str | None = Field(
        default=None,
        json_schema_extra={"enum": ["multi_use", "single_use", "non_replayable", None]},
    )
    capability: str | None = Field(
        default=None,
        json_schema_extra={"enum": ["idempotent", "queryable", "blind", None]},
    )
    provider_idempotency_key_param: str | None = None
    provider_idempotency_key_ttl: float | None = Field(default=None, gt=0)
    propagate_effect_id_as_provider_key: bool = False
    request_id_from: str | None = None
    loop_guard: bool | dict[str, Any] | None = None
    budget_guard: bool | None = None
    scope_guard: bool | dict[str, Any] | None = None
    state_authority: bool | dict[str, Any] | None = None
    secret_fields: list[str] = Field(default_factory=list)
    secret_args: bool | None = None
    entity_guard: bool | None = None
    destructive_confirm: bool | None = None
    use_time_currency: bool | None = None


class TaskConfigModel(_ConfigModel):
    """Configuration for one application task."""

    callable: str | None = Field(
        default=None,
        json_schema_extra={
            "pattern": (
                r"^(?:[A-Za-z_][A-Za-z0-9_]*\.)*"
                r"[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*$"
            )
        },
    )
    ledger: bool | dict[str, Any] | None = None
    id_from: list[str] | None = None
    audit_receipt: bool = False


class RegistryConfigModel(_ConfigModel):
    allowed: list[str] = Field(default_factory=list)
    auto: bool = False


class RunnerConfigModel(_ConfigModel):
    max_llm_retries: int | None = Field(default=None, ge=0)
    max_tool_retries: int | None = Field(default=None, ge=0)


class HistoryGuardConfigModel(_ConfigModel):
    max_tokens: int | None = Field(default=None, gt=0)
    max_messages: int | None = Field(default=None, gt=0)


class MessageValidatorConfigModel(_ConfigModel):
    enabled: bool = True


class LangGraphIntegrationConfigModel(_ConfigModel):
    enabled: bool = True


class IntegrationsConfigModel(_ConfigModel):
    langgraph: bool | LangGraphIntegrationConfigModel | None = None


class DeploymentConfigModel(_ConfigModel):
    topology: str | None = Field(
        default=None,
        json_schema_extra={"enum": ["single_node", "multi_node", None]},
    )


class SecretArgsConfigModel(_ConfigModel):
    enabled: bool = True
    policy: str = Field(
        default="error",
        json_schema_extra={"enum": ["error", "redact", "warn"]},
    )
    allow_fields: list[str] = Field(default_factory=list)
    allow_tools: list[str] = Field(default_factory=list)
    entropy_detection: bool = True


class MyceliumConfigModel(_ConfigModel):
    """Version 1 structural model for a complete ``mycelium.yaml`` file.

    Unknown fields remain accepted for compatibility with existing extension
    sections.  Declared fields are strict, documented, and discoverable by
    JSON-Schema-aware editors and configuration agents.
    """

    model_config = ConfigDict(
        extra="allow",
        strict=True,
        title="Mycelium configuration",
        json_schema_extra={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": CONFIG_SCHEMA_ID,
        },
    )

    config_version: Literal[1] = Field(
        default=CONFIG_VERSION,
        description="Configuration document format. Omitted legacy files are version 1.",
    )
    profile: str = Field(
        default="development",
        json_schema_extra={"enum": ["development", "production"]},
    )
    transition: TransitionConfigModel | None = None
    state_backend: StorageConfigModel | None = None
    action_ledger: LedgerConfigModel | None = None
    task_ledger: TaskLedgerConfigModel | None = None
    state_flush: StorageConfigModel | None = None
    audit_receipt: StorageConfigModel | None = None
    outcome_emit: StorageConfigModel | None = None
    tools: dict[str, ToolConfigModel | None] = Field(default_factory=dict)
    tasks: dict[str, TaskConfigModel | None] = Field(default_factory=dict)
    registry: RegistryConfigModel = Field(default_factory=RegistryConfigModel)
    runner: RunnerConfigModel = Field(default_factory=RunnerConfigModel)
    history_guard: HistoryGuardConfigModel | None = None
    message_validator: bool | MessageValidatorConfigModel = False
    integrations: IntegrationsConfigModel | None = None
    loop_guard: StorageConfigModel | None = None
    budget: BudgetConfigModel | None = None
    scope_guard: StorageConfigModel | None = None
    state_authority: dict[str, Any] | None = None
    completion: CompletionConfigModel | None = None
    deployment: DeploymentConfigModel | None = None
    verify: dict[str, Any] | None = None
    secret_args: SecretArgsConfigModel | None = None
    entity_guard: dict[str, Any] | None = None
    destructive_confirm: dict[str, Any] | None = None
    authority_window: dict[str, Any] | None = None
    use_time_currency: dict[str, Any] | None = None

    @field_validator("config_version", mode="before")
    @classmethod
    def _supported_config_version(cls, value: Any) -> Any:
        if value != CONFIG_VERSION:
            raise ValueError(
                f"unsupported config_version {value!r}; this Mycelium "
                f"runtime supports version {CONFIG_VERSION}. Upgrade Mycelium "
                "or migrate the file after reviewing the release notes"
            )
        return value


def config_json_schema() -> dict[str, Any]:
    """Return the versioned JSON Schema used by agents and IDEs."""

    return MyceliumConfigModel.model_json_schema(mode="validation")


def validate_config_shape(data: Any) -> MyceliumConfigModel:
    """Validate version and declared field shapes without constructing runtime guards."""

    return MyceliumConfigModel.model_validate(data)


def format_validation_error(exc: ValidationError) -> str:
    """Render stable, YAML-oriented diagnostics from a Pydantic error."""

    diagnostics: list[str] = []
    for error in exc.errors(include_url=False):
        location = ""
        for part in error["loc"]:
            if isinstance(part, int):
                location += f"[{part}]"
            else:
                location += ("." if location else "") + str(part)
        diagnostics.append(f"{location or '<root>'}: {error['msg']}")
    return "; ".join(diagnostics)


__all__ = [
    "CONFIG_SCHEMA_ID",
    "CONFIG_VERSION",
    "MyceliumConfigModel",
    "config_json_schema",
    "format_validation_error",
    "validate_config_shape",
]
