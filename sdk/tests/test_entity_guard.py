"""Destination-policy guard: unauthorized writes never reach claim."""

from __future__ import annotations

import json
from typing import Any

import pytest

from mycelium import (
    PAYLOAD_OMITTED,
    ConfigError,
    EntityGuardError,
    InMemoryLedgerStorage,
    apply_entity_guard,
    canonicalize_email,
    canonicalize_https_url,
    enforce_entity_guard,
    ledger_sync,
    load_config_from_string,
)
from mycelium.action_ledger import ARGS_DRIFT_HARD
from mycelium.doctor.types import DoctorStatus
from mycelium.entity_guard import (
    DEST_EMAIL,
    DEST_ENTITY_ID,
    DEST_HTTPS_URL,
    DestinationAllow,
    DestinationSpec,
    EntityGuardPolicy,
    ToolDestinationPolicy,
    reset_entity_guard_state,
    sanitize_entity_evidence,
)
from mycelium.transition import (
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    args_fingerprint,
    execution_scope,
)


@pytest.fixture(autouse=True)
def _reset() -> None:
    reset_entity_guard_state()
    yield
    reset_entity_guard_state()


def _policy(**tools: ToolDestinationPolicy) -> EntityGuardPolicy:
    return EntityGuardPolicy(
        enabled=True,
        missing_policy="error",
        policy_version="test",
        tools=dict(tools),
    )


def _email_tool() -> ToolDestinationPolicy:
    return ToolDestinationPolicy(
        destinations=(
            DestinationSpec(
                path="recipient",
                dest_type=DEST_EMAIL,
                allow=DestinationAllow(
                    addresses=frozenset({"billing@customer.com"}),
                    domains=frozenset({"customer.com"}),
                ),
            ),
            DestinationSpec(
                path="cc",
                dest_type=DEST_EMAIL,
                allow=DestinationAllow(),
                required=False,
            ),
        )
    )


def _url_tool() -> ToolDestinationPolicy:
    return ToolDestinationPolicy(
        destinations=(
            DestinationSpec(
                path="url",
                dest_type=DEST_HTTPS_URL,
                allow=DestinationAllow(
                    hosts=frozenset({"api.stripe.com", "hooks.slack.com"})
                ),
                reject_redirects=True,
            ),
        )
    )


def _ticket_tool() -> ToolDestinationPolicy:
    return ToolDestinationPolicy(
        destinations=(
            DestinationSpec(
                path="project_id",
                dest_type=DEST_ENTITY_ID,
                allow=DestinationAllow(values=frozenset({"SUPPORT", "INCIDENTS"})),
            ),
        )
    )


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="t",
        policy_version="test",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def test_omitted_config_keeps_existing_behavior() -> None:
    cfg = load_config_from_string(
        """
tools:
  send_email:
    side_effect_class: non_idempotent_mutate
"""
    )
    assert cfg.entity_guard is None
    assert cfg.entity_guard_applies("send_email") is False


def test_canonical_email_and_https() -> None:
    assert canonicalize_email("Billing@Customer.COM") == "billing@customer.com"
    assert (
        canonicalize_https_url("https://API.Stripe.com/v1")
        == "https://api.stripe.com/v1"
    )


def test_allow_email_and_domain() -> None:
    policy = _policy(send_email=_email_tool())

    def send_email(recipient: str, body: str) -> str:
        return recipient

    wrapped = apply_entity_guard(send_email, policy, tool_name="send_email")
    assert wrapped(recipient="billing@customer.com", body="secret") == "billing@customer.com"
    assert wrapped(recipient="ops@customer.com", body="secret") == "ops@customer.com"


def test_forbidden_and_missing_fail_closed() -> None:
    policy = _policy(send_email=_email_tool())

    def send_email(recipient: str | None = None, body: str = "") -> str:
        return "ran"

    wrapped = apply_entity_guard(send_email, policy, tool_name="send_email")
    with pytest.raises(EntityGuardError) as denied:
        wrapped(recipient="exfil@evil.example", body="secret")
    assert denied.value.reason == "not_allowed"
    with pytest.raises(EntityGuardError) as missing:
        wrapped(body="secret")
    assert missing.value.reason == "missing"


def test_empty_cc_allowlist_denies_present_values() -> None:
    policy = _policy(send_email=_email_tool())

    def send_email(recipient: str, body: str, cc: list[str] | None = None) -> str:
        return "ran"

    wrapped = apply_entity_guard(send_email, policy, tool_name="send_email")
    assert wrapped(recipient="billing@customer.com", body="x") == "ran"
    with pytest.raises(EntityGuardError) as caught:
        wrapped(recipient="billing@customer.com", body="x", cc=["other@customer.com"])
    assert caught.value.reason == "not_allowed"


def test_undeclared_bcc_fails_closed() -> None:
    policy = _policy(send_email=_email_tool())

    def send_email(recipient: str, body: str, bcc: str | None = None) -> str:
        return "ran"

    wrapped = apply_entity_guard(send_email, policy, tool_name="send_email")
    with pytest.raises(EntityGuardError) as caught:
        wrapped(recipient="billing@customer.com", body="x", bcc="shadow@evil.example")
    assert caught.value.reason == "undeclared"


def test_dynamic_and_encoded_url_rejected() -> None:
    policy = _policy(http_post=_url_tool())

    def http_post(url: str, body: str) -> str:
        return "ran"

    wrapped = apply_entity_guard(http_post, policy, tool_name="http_post")
    with pytest.raises(EntityGuardError) as dynamic:
        wrapped(url="https://{host}/v1", body="x")
    assert dynamic.value.reason == "dynamic"
    with pytest.raises(EntityGuardError):
        wrapped(url="https://api.stripe.com.evil.example/v1", body="x")
    with pytest.raises(EntityGuardError):
        wrapped(url="https://evil.example@api.stripe.com/", body="x")
    with pytest.raises(EntityGuardError):
        wrapped(url="http://api.stripe.com/v1", body="x")
    with pytest.raises(EntityGuardError):
        wrapped(url="https://api.stripe.com/go?next=https://evil.example", body="x")
    assert wrapped(url="https://API.stripe.com/v1", body="x") == "ran"


def test_entity_id_and_ticket_project() -> None:
    policy = _policy(create_ticket=_ticket_tool())

    def create_ticket(project_id: str, body: str) -> str:
        return project_id

    wrapped = apply_entity_guard(create_ticket, policy, tool_name="create_ticket")
    assert wrapped(project_id="support", body="x") == "support"
    with pytest.raises(EntityGuardError):
        wrapped(project_id="PAYROLL", body="x")


def test_blocks_before_ledger_claim() -> None:
    events: list[str] = []

    class Recording(InMemoryLedgerStorage):
        def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
            events.append("claim")
            return super().try_claim_inflight(entry, lease_ttl=lease_ttl)

    def send_email(recipient: str, body: str) -> str:
        events.append("body")
        return "ran"

    wrapped = apply_entity_guard(
        ledger_sync(storage=Recording(), transition_binding=_binding())(send_email),
        _policy(send_email=_email_tool()),
        tool_name="send_email",
    )
    with execution_scope(TransitionScope(run_id="r", thread_id="t")):
        with pytest.raises(EntityGuardError):
            wrapped(recipient="exfil@evil.example", body="payroll", request_id="eg-1")
    assert events == []


def test_canonical_dest_bound_into_fingerprint() -> None:
    policy = _policy(send_email=_email_tool())

    def send_email(recipient: str, body: str) -> str:
        return recipient

    _args, kwargs_a, decision_a = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "Billing@Customer.COM", "body": "one"},
        policy=policy,
        func=send_email,
    )
    _args, kwargs_b, decision_b = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "billing@customer.com", "body": "two"},
        policy=policy,
        func=send_email,
    )
    assert kwargs_a["recipient"] == kwargs_b["recipient"] == "billing@customer.com"
    assert args_fingerprint((), {"recipient": kwargs_a["recipient"]}) == args_fingerprint(
        (), {"recipient": kwargs_b["recipient"]}
    )
    _args, kwargs_c, _dec = enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "ops@customer.com", "body": "three"},
        policy=policy,
        func=send_email,
    )
    assert kwargs_c["recipient"] != kwargs_a["recipient"]
    del decision_a, decision_b


def test_retry_cannot_change_recipient_on_same_request_id() -> None:
    storage = InMemoryLedgerStorage()

    def send_email(recipient: str, body: str) -> str:
        return recipient

    wrapped = apply_entity_guard(
        ledger_sync(
            storage=storage,
            transition_binding=_binding(),
            on_args_drift=ARGS_DRIFT_HARD,
        )(send_email),
        _policy(send_email=_email_tool()),
        tool_name="send_email",
    )
    with execution_scope(TransitionScope(run_id="r", thread_id="t")):
        assert (
            wrapped(
                recipient="billing@customer.com",
                body="first",
                request_id="eg-drift",
            )
            == "billing@customer.com"
        )
        with pytest.raises(Exception, match="[Dd]rift|args"):
            wrapped(
                recipient="ops@customer.com",
                body="first",
                request_id="eg-drift",
            )


def test_evidence_omits_payload() -> None:
    policy = _policy(send_email=_email_tool())
    storage = InMemoryLedgerStorage()

    def send_email(recipient: str, body: str) -> str:
        return recipient

    wrapped = apply_entity_guard(
        ledger_sync(storage=storage, transition_binding=_binding())(send_email),
        policy,
        tool_name="send_email",
    )
    with execution_scope(TransitionScope(run_id="r", thread_id="t")):
        wrapped(
            recipient="billing@customer.com",
            body="INTERNAL_PAYROLL",
            request_id="eg-ev",
        )
    entry = storage.get("eg-ev")
    assert entry is not None
    dumped = json.dumps(entry.to_dict(), default=str)
    assert "INTERNAL_PAYROLL" not in dumped
    assert PAYLOAD_OMITTED in dumped or "billing@customer.com" in dumped


def test_sanitize_entity_evidence_drops_body() -> None:
    policy = _policy(send_email=_email_tool())
    enforce_entity_guard(
        "send_email",
        (),
        {"recipient": "billing@customer.com", "body": "secret-payroll"},
        policy=policy,
        func=lambda recipient, body: None,
    )
    _args, kwargs = sanitize_entity_evidence(
        (),
        {"recipient": "billing@customer.com", "body": "secret-payroll"},
    )
    assert kwargs["body"] == PAYLOAD_OMITTED
    assert kwargs["recipient"] == "billing@customer.com"
    assert "secret-payroll" not in json.dumps(kwargs)


def test_sync_and_async_wrappers_match() -> None:
    policy = _policy(send_email=_email_tool())

    def sync_send(recipient: str, body: str) -> str:
        return recipient

    async def async_send(recipient: str, body: str) -> str:
        return recipient

    sync_wrapped = apply_entity_guard(sync_send, policy, tool_name="send_email")
    async_wrapped = apply_entity_guard(async_send, policy, tool_name="send_email")
    assert sync_wrapped(recipient="billing@customer.com", body="x") == "billing@customer.com"

    import asyncio

    assert (
        asyncio.run(async_wrapped(recipient="Billing@Customer.com", body="x"))
        == "billing@customer.com"
    )


def test_yaml_example_and_production_requires_error() -> None:
    cfg = load_config_from_string(
        """
entity_guard:
  enabled: true
  missing_policy: error
  tools:
    send_email:
      destinations:
        - path: recipient
          type: email
          allow:
            addresses: [billing@customer.com]
            domains: [customer.com]
        - path: cc
          type: email
          allow: []
          required: false
    http_post:
      destinations:
        - path: url
          type: https_url
          allow:
            hosts: [api.stripe.com, hooks.slack.com]
          reject_redirects: true
    create_ticket:
      destinations:
        - path: project_id
          type: entity_id
          allow:
            values: [SUPPORT, INCIDENTS]
tools:
  send_email:
    side_effect_class: non_idempotent_mutate
  http_post:
    side_effect_class: non_idempotent_mutate
  create_ticket:
    side_effect_class: non_idempotent_mutate
"""
    )
    assert cfg.entity_guard_applies("send_email") is True
    assert cfg.entity_guard_applies("search") is False

    with pytest.raises(ConfigError, match="entity_guard.missing_policy"):
        load_config_from_string(
            """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [send_email]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
entity_guard:
  enabled: true
  missing_policy: warn
  tools:
    send_email:
      destinations:
        - path: recipient
          type: email
          allow:
            addresses: [billing@customer.com]
tools:
  send_email:
    side_effect_class: non_idempotent_mutate
"""
        )


def test_unknown_yaml_keys_rejected() -> None:
    with pytest.raises(ConfigError, match="unsupported"):
        load_config_from_string("entity_guard:\n  llm_allowlist: true\n")


def test_doctor_omitted_is_skip(tmp_path: Any) -> None:
    from mycelium import run_doctor

    path = tmp_path / "mycelium.yaml"
    path.write_text(
        """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [charge]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
tools:
  charge:
    side_effect_class: non_idempotent_mutate
""",
        encoding="utf-8",
    )
    report = run_doctor(path, connectivity=False)
    scanning = next(c for c in report.checks if c.id == "entity.scanning")
    assert scanning.status == DoctorStatus.SKIP
