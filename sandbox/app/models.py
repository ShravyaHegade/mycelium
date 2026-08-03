"""Sandbox request/response models (no LLM — deterministic plans)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

ToolName = Literal[
    "charge",
    "refund",
    "send_email",
    "ship_order",
    "create_ticket",
    "post_slack",
    "search_docs",
    "lookup_order",
    "set_status",
    "charge_keyed",
    "delete_account",
]
ToolOutcome = Literal[
    "success",
    "timeout_after_effect",
    "slow",
    "fail_before",
    "custom_ok",
    "custom_fail",
]
SideEffectClass = Literal[
    "read",
    "idempotent_mutate",
    "keyed_mutate",
    "non_idempotent_mutate",
    "irreversible",
]
Injector = Literal["none", "redispatch", "peer_slow", "crash_hard_block"]
RunMode = Literal["without", "with", "both"]


class ToolSpec(BaseModel):
    """Default catalog entry for a tool type (YAML / defaults)."""

    name: ToolName
    outcome: ToolOutcome = "success"
    side_effect_class: SideEffectClass = "non_idempotent_mutate"
    amount: float = 10.0
    query: str = "billing"
    to: str = "user@example.com"
    order_id: str = "ord_1001"
    subject: str = "Need help with my order"
    channel: str = "#ops"
    status: str = "processing"
    idempotency_key: str = "pay_key_1"
    account_id: str = "acct_1"


class PlanStep(BaseModel):
    """One node in the user's graph — outcomes are free per step."""

    tool: ToolName
    tool_call_id: str = "call_1"
    outcome: ToolOutcome | None = None
    side_effect_class: SideEffectClass | None = None
    amount: float | None = None
    query: str | None = None
    to: str | None = None
    order_id: str | None = None
    subject: str | None = None
    channel: str | None = None
    status: str | None = None
    idempotency_key: str | None = None
    account_id: str | None = None
    # optional graph metadata (ignored by runner)
    id: str | None = None


class RunRequest(BaseModel):
    tools: list[ToolSpec] = Field(default_factory=list)
    plan: list[PlanStep]
    injector: Injector = "none"
    mode: RunMode = "both"
    agent_id: str = "sandbox-agent"
    policy_version: str = "2026.08.1"


class Event(BaseModel):
    kind: str
    message: str
    detail: dict[str, Any] = Field(default_factory=dict)


class RunResult(BaseModel):
    mode: Literal["without", "with"]
    ok: bool
    executions: dict[str, int]
    events: list[Event]
    gate: str | None = None
    error: str | None = None
    yaml_used: str | None = None


class RunResponse(BaseModel):
    results: list[RunResult]
    yaml_preview: str


class YamlPreviewRequest(BaseModel):
    tools: list[ToolSpec] = Field(default_factory=list)
    plan: list[PlanStep] = Field(default_factory=list)
    agent_id: str = "sandbox-agent"
    policy_version: str = "2026.08.1"


class CliDemoRequest(BaseModel):
    tools: list[ToolSpec] = Field(default_factory=list)
    plan: list[PlanStep]
    injector: Injector = "none"
    agent_id: str = "sandbox-agent"
    policy_version: str = "2026.08.1"
