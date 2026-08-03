"""Build a minimal mycelium.yaml from wizard tool specs / plan nodes."""

from __future__ import annotations

from app.models import PlanStep, SideEffectClass, ToolSpec


_DEFAULT_CLASS: dict[str, SideEffectClass] = {
    "search_docs": "read",
    "lookup_order": "read",
    "set_status": "idempotent_mutate",
    "charge_keyed": "keyed_mutate",
    "charge": "non_idempotent_mutate",
    "refund": "non_idempotent_mutate",
    "send_email": "non_idempotent_mutate",
    "ship_order": "non_idempotent_mutate",
    "create_ticket": "non_idempotent_mutate",
    "post_slack": "non_idempotent_mutate",
    "delete_account": "irreversible",
}


def tools_from_plan(
    plan: list[PlanStep],
    tools: list[ToolSpec] | None = None,
) -> list[ToolSpec]:
    """Merge plan nodes into unique ToolSpecs for YAML."""
    by_name: dict[str, ToolSpec] = {t.name: t for t in (tools or [])}
    for step in plan:
        prev = by_name.get(step.tool)
        sec = (
            step.side_effect_class
            or (prev.side_effect_class if prev else None)
            or _DEFAULT_CLASS.get(step.tool, "non_idempotent_mutate")
        )
        outcome = step.outcome or (prev.outcome if prev else "success")
        by_name[step.tool] = ToolSpec(
            name=step.tool,
            outcome=outcome,
            side_effect_class=sec,
            amount=step.amount if step.amount is not None else (prev.amount if prev else 10.0),
            query=step.query if step.query is not None else (prev.query if prev else "billing"),
            to=step.to if step.to is not None else (prev.to if prev else "user@example.com"),
            order_id=(
                step.order_id
                if step.order_id is not None
                else (prev.order_id if prev else "ord_1001")
            ),
            subject=(
                step.subject
                if step.subject is not None
                else (prev.subject if prev else "Need help with my order")
            ),
            channel=(
                step.channel
                if step.channel is not None
                else (prev.channel if prev else "#ops")
            ),
            status=(
                step.status
                if step.status is not None
                else (prev.status if prev else "processing")
            ),
            idempotency_key=(
                step.idempotency_key
                if step.idempotency_key is not None
                else (prev.idempotency_key if prev else "pay_key_1")
            ),
            account_id=(
                step.account_id
                if step.account_id is not None
                else (prev.account_id if prev else "acct_1")
            ),
        )
    if not by_name:
        by_name["charge"] = ToolSpec(name="charge")
    return list(by_name.values())


def build_yaml(
    tools: list[ToolSpec],
    *,
    agent_id: str = "sandbox-agent",
    policy_version: str = "2026.08.1",
    plan: list[PlanStep] | None = None,
) -> str:
    if plan:
        tools = tools_from_plan(plan, tools)
    if not tools:
        tools = [ToolSpec(name="charge", side_effect_class="non_idempotent_mutate")]

    tool_names = sorted({t.name for t in tools})
    class_by_name = {t.name: t.side_effect_class for t in tools}

    lines = [
        "transition:",
        f"  agent_id: {agent_id}",
        f'  policy_version: "{policy_version}"',
        "",
        "action_ledger:",
        "  storage: memory",
        "  tools:",
    ]
    for name in tool_names:
        lines.append(f"    - {name}")
    lines.append("")
    lines.append("tools:")
    for name in tool_names:
        sec = class_by_name.get(name, "non_idempotent_mutate")
        lines.append(f"  {name}:")
        lines.append(f"    side_effect_class: {sec}")
    lines.append("")
    return "\n".join(lines)
