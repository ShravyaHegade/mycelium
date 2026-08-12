"""Production-stable business request identity for consequential tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from mycelium import (
    PROFILE_PRODUCTION,
    REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    ConfigError,
    InMemoryLedgerStorage,
    MissingRequestIdentityError,
    SideEffectClass,
    SqliteLedgerStorage,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    get_ledger,
    ledger_sync,
    load_config_from_string,
)

_BINDING = ToolTransitionBinding.for_tool(
    agent_id="rid-policy",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)


def _prod_yaml(tmp_path: Path, extra: str = "", tool_extra: str = "") -> str:
    return f"""
profile: production
transition:
  agent_id: rid-agent
  policy_version: "2026.08.1"
action_ledger:
  storage: sqlite
  path: {tmp_path / "ledger.db"}
  tools: [charge, search]
outcome_emit:
  storage: file
  path: {tmp_path / "outcomes.jsonl"}
tools:
  charge:
    side_effect_class: non_idempotent_mutate
{tool_extra}
  search:
    side_effect_class: read
{extra}
"""


def test_production_side_effecting_without_business_id_fails_before_claim(
    tmp_path: Path,
) -> None:
    cfg = load_config_from_string(_prod_yaml(tmp_path))
    calls = {"n": 0}

    def charge(amount: int = 1) -> str:
        calls["n"] += 1
        return "paid"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-1")):
        with pytest.raises(MissingRequestIdentityError, match="charge") as exc:
            wrapped(amount=1, tool_call_id="tc-new")
    assert calls["n"] == 0
    assert "server-owned" in str(exc.value)
    ledger = get_ledger(wrapped)
    assert ledger is not None
    assert ledger._storage.list_all() == []


def test_production_read_without_request_id_is_allowed(tmp_path: Path) -> None:
    cfg = load_config_from_string(_prod_yaml(tmp_path))
    calls = {"n": 0}

    def search(q: str = "x") -> str:
        calls["n"] += 1
        return q

    wrapped = cfg.apply_tool("search", search)
    with execution_scope(TransitionScope(run_id="run-read")):
        assert wrapped(q="hi", tool_call_id="tc-read") == "hi"
    assert calls["n"] == 1


def test_stable_request_id_retries_deduplicate(tmp_path: Path) -> None:
    cfg = load_config_from_string(_prod_yaml(tmp_path))
    calls = {"n": 0}

    def charge(amount: int = 1) -> str:
        calls["n"] += 1
        return f"paid-{amount}"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-dedup")):
        assert wrapped(amount=5, request_id="charge:ORD-1") == "paid-5"
        assert wrapped(amount=5, request_id="charge:ORD-1") == "paid-5"
    assert calls["n"] == 1


def test_request_id_from_is_stable_across_retries(tmp_path: Path) -> None:
    cfg = load_config_from_string(
        _prod_yaml(tmp_path, tool_extra="    request_id_from: order_id\n")
    )
    calls: list[str] = []

    def charge(order_id: str) -> str:
        calls.append(order_id)
        return f"paid-{order_id}"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-from")):
        first = wrapped(order_id="ORD-123")
        replay = wrapped(order_id="ORD-123")
    assert first == replay == "paid-ORD-123"
    assert calls == ["ORD-123"]
    ledger = get_ledger(wrapped)
    assert ledger is not None
    entry = ledger.get("charge:order_id:ORD-123")
    assert entry is not None


def test_different_business_ids_are_different_transitions(tmp_path: Path) -> None:
    cfg = load_config_from_string(
        _prod_yaml(tmp_path, tool_extra="    request_id_from: order_id\n")
    )
    calls: list[str] = []

    def charge(order_id: str) -> str:
        calls.append(order_id)
        return order_id

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-diff")):
        wrapped(order_id="ORD-1")
        wrapped(order_id="ORD-2")
    assert calls == ["ORD-1", "ORD-2"]


def test_empty_or_missing_request_id_from_fails_before_execution(
    tmp_path: Path,
) -> None:
    cfg = load_config_from_string(
        _prod_yaml(tmp_path, tool_extra="    request_id_from: order_id\n")
    )
    calls = {"n": 0}

    def charge(order_id: str | None = None) -> str:
        calls["n"] += 1
        return "nope"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-empty")):
        with pytest.raises(MissingRequestIdentityError, match="order_id"):
            wrapped()
        with pytest.raises(MissingRequestIdentityError, match="order_id"):
            wrapped(order_id="")
        with pytest.raises(MissingRequestIdentityError, match="order_id"):
            wrapped(order_id="   ")
    assert calls["n"] == 0
    assert get_ledger(wrapped)._storage.list_all() == []  # type: ignore[union-attr]


def test_tool_call_id_alone_does_not_satisfy_production(tmp_path: Path) -> None:
    cfg = load_config_from_string(_prod_yaml(tmp_path))
    calls = {"n": 0}

    def charge() -> str:
        calls["n"] += 1
        return "paid"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-tc")):
        with pytest.raises(MissingRequestIdentityError):
            wrapped(tool_call_id="looks-stable")
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_async_yaml_apply_matches_sync(tmp_path: Path) -> None:
    cfg = load_config_from_string(_prod_yaml(tmp_path))

    async def charge(amount: int = 1) -> str:
        return "async-paid"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-async")):
        with pytest.raises(MissingRequestIdentityError):
            await wrapped(amount=1, tool_call_id="tc-async")
        assert await wrapped(amount=1, request_id="charge:ORD-async") == "async-paid"


def test_sqlite_restart_same_business_id_does_not_reexecute(tmp_path: Path) -> None:
    db = tmp_path / "restart.db"
    binding = ToolTransitionBinding.for_tool(
        agent_id="rid-policy",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        request_id_from="order_id",
    )
    calls: list[str] = []

    def charge(order_id: str) -> str:
        calls.append(order_id)
        return order_id

    first = ledger_sync(
        storage=SqliteLedgerStorage(db),
        transition_binding=binding,
        request_identity_policy=REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    )(charge)
    first(order_id="ORD-9")

    second = ledger_sync(
        storage=SqliteLedgerStorage(db),
        transition_binding=binding,
        request_identity_policy=REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    )(charge)
    assert second(order_id="ORD-9") == "ORD-9"
    assert calls == ["ORD-9"]


def test_development_default_still_derives_identity() -> None:
    calls = {"n": 0}

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=_BINDING)
    def charge(amount: int = 1) -> str:
        calls["n"] += 1
        return "ok"

    charge(amount=1, tool_call_id="derived-1")
    charge(amount=1, tool_call_id="derived-1")
    assert calls["n"] == 1


def test_production_rejects_weaker_derived_policy(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="request_identity_policy"):
        load_config_from_string(
            f"""
profile: production
action_ledger:
  storage: sqlite
  path: {tmp_path / "ledger.db"}
  request_identity_policy: derived
  tools: [charge]
outcome_emit:
  storage: file
  path: {tmp_path / "outcomes.jsonl"}
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
        )


def test_production_default_policy_is_require_explicit(tmp_path: Path) -> None:
    cfg = load_config_from_string(_prod_yaml(tmp_path))
    assert cfg.profile == PROFILE_PRODUCTION
    wrapped = cfg.apply_tool("charge", lambda: "x")
    ledger = get_ledger(wrapped)
    assert ledger is not None
    assert (
        ledger._request_identity_policy
        == REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT
    )


def test_langgraph_path_requires_business_id(tmp_path: Path) -> None:
    cfg = load_config_from_string(
        _prod_yaml(
            tmp_path,
            extra="""
integrations:
  langgraph:
    enabled: true
""",
        )
    )
    calls = {"n": 0}

    def charge(amount: int = 1) -> str:
        calls["n"] += 1
        return "paid"

    wrapped = cfg.apply_tool("charge", charge)
    with execution_scope(TransitionScope(run_id="run-lg")):
        with pytest.raises(MissingRequestIdentityError):
            wrapped(amount=1, tool_call_id="lg-tc")
        assert wrapped(amount=1, request_id="charge:ORD-lg") == "paid"
    assert calls["n"] == 1


def test_existing_development_config_unchanged() -> None:
    cfg = load_config_from_string(
        """
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    )
    assert cfg.profile != PROFILE_PRODUCTION
    assert cfg.outcome_emit is None
