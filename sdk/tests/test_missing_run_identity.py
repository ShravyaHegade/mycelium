"""Opt-in missing_run_id_policy for LoopGuard and ScopeGuard."""

from __future__ import annotations

import asyncio
import warnings

import pytest

from mycelium import (
    MISSING_RUN_ID_POLICY_ERROR,
    MISSING_RUN_ID_POLICY_WARN,
    ConfigError,
    InMemoryLedgerStorage,
    InMemoryLoopGuardStorage,
    InMemoryScopeGuardStorage,
    LoopGuard,
    MissingRunIdentityError,
    ScopeGrant,
    ScopeGuard,
    SideEffectClass,
    TransitionScope,
    execution_scope,
    get_ledger,
    load_config_from_string,
    loop_guard,
    loop_guard_sync,
    parse_run_identity,
    resolve_loop_scope_key,
    resolve_run_id,
    scope_guard,
    scope_guard_sync,
)
from mycelium.loop_guard import reset_missing_run_identity_warnings
from mycelium.tool_boundary import ToolBoundaryError


def _scope(run_id: str = "run-1", thread_id: str = "t1") -> TransitionScope:
    return TransitionScope(thread_id=thread_id, run_id=run_id, node="tools")


def _loop_guard(
    *,
    policy: str = MISSING_RUN_ID_POLICY_WARN,
    consecutive_soft: dict[str, int] | None = None,
) -> LoopGuard:
    return LoopGuard(
        InMemoryLoopGuardStorage(),
        missing_run_id_policy=policy,
        consecutive_soft=consecutive_soft,
    )


def _scope_guard(*, policy: str = MISSING_RUN_ID_POLICY_WARN) -> ScopeGuard:
    return ScopeGuard(
        InMemoryScopeGuardStorage(),
        default_grant=ScopeGrant(allowed_tools=frozenset({"search", "fetch"})),
        missing_run_id_policy=policy,
    )


@pytest.fixture(autouse=True)
def _reset_identity_warnings() -> None:
    reset_missing_run_identity_warnings()
    yield
    reset_missing_run_identity_warnings()


# ---------------------------------------------------------------------------
# A. Configuration
# ---------------------------------------------------------------------------


def test_omitted_policy_defaults_to_warn() -> None:
    cfg = load_config_from_string(
        """
loop_guard:
  storage: memory
scope_guard:
  storage: memory
  allowed_tools: [search]
tools:
  search:
    side_effect_class: read
"""
    )
    loop = cfg.build_loop_guard()
    scope = cfg.build_scope_guard()
    assert loop is not None
    assert scope is not None
    assert loop.missing_run_id_policy == MISSING_RUN_ID_POLICY_WARN
    assert scope.missing_run_id_policy == MISSING_RUN_ID_POLICY_WARN


@pytest.mark.parametrize("policy", ["warn", "error"])
def test_policy_parses(policy: str) -> None:
    cfg = load_config_from_string(
        f"""
loop_guard:
  storage: memory
  missing_run_id_policy: {policy}
scope_guard:
  storage: memory
  allowed_tools: [search]
  missing_run_id_policy: {policy}
tools:
  search:
    side_effect_class: read
"""
    )
    loop = cfg.build_loop_guard()
    scope = cfg.build_scope_guard()
    assert loop is not None and loop.missing_run_id_policy == policy
    assert scope is not None and scope.missing_run_id_policy == policy


def test_invalid_policy_raises_config_error() -> None:
    with pytest.raises(
        ConfigError,
        match=(
            r"'loop_guard.missing_run_id_policy' must be "
            r"'warn' or 'error', got 'allow'"
        ),
    ):
        load_config_from_string(
            """
loop_guard:
  storage: memory
  missing_run_id_policy: allow
tools:
  search:
    side_effect_class: read
"""
        )
    with pytest.raises(
        ConfigError,
        match=(
            r"'scope_guard.missing_run_id_policy' must be "
            r"'warn' or 'error', got 'strict'"
        ),
    ):
        load_config_from_string(
            """
scope_guard:
  storage: memory
  allowed_tools: [search]
  missing_run_id_policy: strict
tools:
  search:
    side_effect_class: read
"""
        )


def test_policy_key_does_not_leak_into_storage_constructor(tmp_path) -> None:
    path = tmp_path / "loop.json"
    cfg = load_config_from_string(
        f"""
loop_guard:
  storage: file
  path: {path}
  missing_run_id_policy: error
scope_guard:
  storage: memory
  allowed_tools: [search]
  missing_run_id_policy: error
tools:
  search:
    side_effect_class: read
"""
    )
    loop = cfg.build_loop_guard()
    scope = cfg.build_scope_guard()
    assert loop is not None
    assert scope is not None
    assert not hasattr(loop.storage, "missing_run_id_policy")
    assert not hasattr(scope.storage, "missing_run_id_policy")
    assert loop.missing_run_id_policy == MISSING_RUN_ID_POLICY_ERROR
    assert scope.missing_run_id_policy == MISSING_RUN_ID_POLICY_ERROR


def test_existing_config_without_new_field_still_loads() -> None:
    cfg = load_config_from_string(
        """
tools:
  search:
    side_effect_class: read
"""
    )
    assert cfg.build_loop_guard() is None
    assert cfg.build_scope_guard() is None


# ---------------------------------------------------------------------------
# Shared resolution
# ---------------------------------------------------------------------------


def test_parse_run_identity_rejects_empty_and_whitespace() -> None:
    assert parse_run_identity(None) is None
    assert parse_run_identity("") is None
    assert parse_run_identity("   ") is None
    assert parse_run_identity(123) is None
    assert parse_run_identity("run-1") == "run-1"


def test_resolve_run_id_does_not_use_thread_id() -> None:
    with execution_scope(_scope(run_id="", thread_id="thread-only")):
        assert resolve_run_id() is None
        assert resolve_loop_scope_key() == "thread-only"


# ---------------------------------------------------------------------------
# B. LoopGuard
# ---------------------------------------------------------------------------


def test_loop_warn_missing_run_id_skips_and_warns_once() -> None:
    guard = _loop_guard()
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str, *, tool_call_id: str) -> str:
        calls["n"] += 1
        return q

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for i in range(8):
            search(q="foo", tool_call_id=f"c{i}")
    assert calls["n"] == 8
    identity_warnings = [w for w in caught if "LoopGuard skipped" in str(w.message)]
    assert len(identity_warnings) == 1


def test_loop_error_missing_run_id_raises_before_tool() -> None:
    guard = _loop_guard(policy=MISSING_RUN_ID_POLICY_ERROR)
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str, *, tool_call_id: str) -> str:
        calls["n"] += 1
        return q

    with pytest.raises(MissingRunIdentityError, match="LoopGuard requires a stable run_id"):
        search(q="foo", tool_call_id="c0")
    assert calls["n"] == 0


@pytest.mark.parametrize("run_id", ["", "   "])
def test_loop_empty_and_whitespace_run_id_are_missing(run_id: str) -> None:
    guard = _loop_guard(policy=MISSING_RUN_ID_POLICY_ERROR)
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str) -> str:
        calls["n"] += 1
        return q

    with execution_scope(_scope(run_id=run_id, thread_id="")):
        with pytest.raises(MissingRunIdentityError):
            search(q="foo")
    assert calls["n"] == 0


def test_loop_error_rejects_thread_id_only() -> None:
    guard = _loop_guard(policy=MISSING_RUN_ID_POLICY_ERROR)
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str) -> str:
        calls["n"] += 1
        return q

    with execution_scope(_scope(run_id="", thread_id="thread-1")):
        with pytest.raises(MissingRunIdentityError):
            search(q="foo")
    assert calls["n"] == 0


def test_loop_stable_run_id_detects_consecutive_actions() -> None:
    guard = _loop_guard(
        consecutive_soft={SideEffectClass.NON_IDEMPOTENT_MUTATE.value: 2}
    )
    calls = {"n": 0}

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(amount: float, *, tool_call_id: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope("run-stable")):
        assert charge(10.0, tool_call_id="c0") == 10.0
        with pytest.raises(ToolBoundaryError):
            charge(10.0, tool_call_id="c1")
    assert calls["n"] == 1


def test_loop_same_run_id_across_retries_shares_state() -> None:
    guard = _loop_guard(
        consecutive_soft={SideEffectClass.NON_IDEMPOTENT_MUTATE.value: 2}
    )
    calls = {"n": 0}

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(amount: float, *, tool_call_id: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope("run-retry")):
        charge(10.0, tool_call_id="c0")
    with execution_scope(_scope("run-retry")):
        with pytest.raises(ToolBoundaryError):
            charge(10.0, tool_call_id="c1")
    assert calls["n"] == 1


def test_loop_different_run_ids_do_not_share_state() -> None:
    guard = _loop_guard(
        consecutive_soft={SideEffectClass.NON_IDEMPOTENT_MUTATE.value: 2}
    )
    calls = {"n": 0}

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(amount: float, *, tool_call_id: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope("run-a")):
        charge(10.0, tool_call_id="c0")
    with execution_scope(_scope("run-b")):
        charge(10.0, tool_call_id="c1")
    assert calls["n"] == 2


# ---------------------------------------------------------------------------
# C. ScopeGuard
# ---------------------------------------------------------------------------


def test_scope_warn_missing_run_id_skips_and_warns_once() -> None:
    guard = _scope_guard()
    calls = {"n": 0}

    @scope_guard_sync(guard, tool_name="delete")
    def delete() -> str:
        calls["n"] += 1
        return "x"

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(6):
            delete()
    assert calls["n"] == 6
    identity_warnings = [w for w in caught if "ScopeGuard skipped" in str(w.message)]
    assert len(identity_warnings) == 1


def test_scope_error_missing_run_id_raises_before_tool() -> None:
    guard = _scope_guard(policy=MISSING_RUN_ID_POLICY_ERROR)
    calls = {"n": 0}

    @scope_guard_sync(guard, tool_name="fetch")
    def fetch() -> str:
        calls["n"] += 1
        return "x"

    with pytest.raises(MissingRunIdentityError, match="ScopeGuard requires a stable run_id"):
        fetch()
    assert calls["n"] == 0


def test_scope_stable_run_id_freezes_and_rechecks() -> None:
    guard = _scope_guard()
    calls = {"n": 0}

    @scope_guard_sync(guard, tool_name="fetch")
    def fetch() -> str:
        calls["n"] += 1
        return "ok"

    @scope_guard_sync(guard, tool_name="delete")
    def delete() -> str:
        calls["n"] += 1
        return "nope"

    with execution_scope(_scope("run-scope")):
        assert fetch() == "ok"
        with pytest.raises(ToolBoundaryError):
            delete()
    assert calls["n"] == 1


def test_scope_same_run_id_shares_frozen_grant() -> None:
    guard = _scope_guard()

    @scope_guard_sync(guard, tool_name="delete")
    def delete() -> str:
        return "nope"

    with execution_scope(_scope("run-freeze")):
        guard.bind()
    with execution_scope(_scope("run-freeze")):
        with pytest.raises(ToolBoundaryError):
            delete()


def test_scope_different_run_ids_have_separate_scopes() -> None:
    storage = InMemoryScopeGuardStorage()
    narrow = ScopeGuard(
        storage,
        default_grant=ScopeGrant(allowed_tools=frozenset({"fetch"})),
    )
    wide = ScopeGuard(
        storage,
        default_grant=ScopeGrant(allowed_tools=frozenset({"fetch", "admin"})),
    )

    @scope_guard_sync(narrow, tool_name="admin")
    def admin_narrow() -> str:
        return "n"

    @scope_guard_sync(wide, tool_name="admin")
    def admin_wide() -> str:
        return "w"

    with execution_scope(_scope("run-narrow")):
        narrow.bind()
        with pytest.raises(ToolBoundaryError):
            admin_narrow()
    with execution_scope(_scope("run-wide")):
        assert admin_wide() == "w"


# ---------------------------------------------------------------------------
# D. Combined guards
# ---------------------------------------------------------------------------


def test_both_enabled_missing_identity_fails_once_without_tool_or_ledger() -> None:
    cfg = load_config_from_string(
        """
transition:
  agent_id: my-agent
  policy_version: "2026.08.1"
loop_guard:
  storage: memory
  missing_run_id_policy: error
scope_guard:
  storage: memory
  allowed_tools: [charge]
  missing_run_id_policy: error
action_ledger:
  storage: memory
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    )
    calls = {"n": 0}

    def charge(*, tool_call_id: str) -> str:
        calls["n"] += 1
        return "paid"

    wrapped = cfg.apply_tool("charge", charge)
    with pytest.raises(MissingRunIdentityError) as exc:
        wrapped(tool_call_id="c0")
    assert calls["n"] == 0
    assert exc.value.guard == "ScopeGuard"
    ledger = get_ledger(wrapped)
    assert ledger is not None
    assert ledger._storage.list_all() == []


def test_combined_warn_is_bounded_not_per_step() -> None:
    cfg = load_config_from_string(
        """
loop_guard:
  storage: memory
scope_guard:
  storage: memory
  allowed_tools: [search]
tools:
  search:
    side_effect_class: read
"""
    )

    def search(*, tool_call_id: str) -> str:
        return "ok"

    wrapped = cfg.apply_tool("search", search)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for i in range(5):
            wrapped(tool_call_id=f"c{i}")
    skipped = [w for w in caught if "skipped:" in str(w.message)]
    assert len(skipped) == 1


def test_wrapper_order_scope_outside_loop_outside_ledger() -> None:
    cfg = load_config_from_string(
        """
transition:
  agent_id: a
  policy_version: p
loop_guard:
  storage: memory
  missing_run_id_policy: error
scope_guard:
  storage: memory
  allowed_tools: [charge]
  missing_run_id_policy: error
action_ledger:
  storage: memory
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    )
    wrapped = cfg.apply_tool("charge", lambda **_: "x")
    assert getattr(wrapped, "_mycelium_scope_guarded", False)
    assert getattr(wrapped, "_mycelium_loop_guarded", False)
    assert get_ledger(wrapped) is not None


# ---------------------------------------------------------------------------
# E. Integration
# ---------------------------------------------------------------------------


def test_explicit_execution_scope_works() -> None:
    guard = _loop_guard(policy=MISSING_RUN_ID_POLICY_ERROR)
    calls = {"n": 0}

    @loop_guard_sync(guard, side_effect_class=SideEffectClass.READ)
    def search(q: str) -> str:
        calls["n"] += 1
        return q

    with execution_scope(TransitionScope(thread_id="thread-1", run_id="run-123")):
        assert search("ok") == "ok"
    assert calls["n"] == 1


def test_sync_and_async_wrappers_match() -> None:
    sync_guard = _loop_guard(policy=MISSING_RUN_ID_POLICY_ERROR)
    async_guard = _loop_guard(policy=MISSING_RUN_ID_POLICY_ERROR)

    @loop_guard_sync(sync_guard, side_effect_class=SideEffectClass.READ)
    def sync_search() -> str:
        return "s"

    @loop_guard(async_guard, side_effect_class=SideEffectClass.READ)
    async def async_search() -> str:
        return "a"

    with pytest.raises(MissingRunIdentityError):
        sync_search()

    async def run() -> None:
        with pytest.raises(MissingRunIdentityError):
            await async_search()
        with execution_scope(_scope("run-async")):
            assert await async_search() == "a"

    asyncio.run(run())

    scope_sync = _scope_guard(policy=MISSING_RUN_ID_POLICY_ERROR)
    scope_async = _scope_guard(policy=MISSING_RUN_ID_POLICY_ERROR)

    @scope_guard_sync(scope_sync, tool_name="fetch")
    def sync_fetch() -> str:
        return "s"

    @scope_guard(scope_async, tool_name="fetch")
    async def async_fetch() -> str:
        return "a"

    with pytest.raises(MissingRunIdentityError):
        sync_fetch()

    async def run_scope() -> None:
        with pytest.raises(MissingRunIdentityError):
            await async_fetch()
        with execution_scope(_scope("run-async-scope")):
            assert await async_fetch() == "a"

    asyncio.run(run_scope())


def test_langgraph_supplied_run_id_enables_guards() -> None:
    from types import SimpleNamespace

    cfg = load_config_from_string(
        """
integrations:
  langgraph: {enabled: true}
transition:
  agent_id: test-agent
  policy_version: "1"
loop_guard:
  storage: memory
  consecutive_soft:
    non_idempotent_mutate: 2
  missing_run_id_policy: error
action_ledger:
  storage: memory
  tools: [send_payment]
tools:
  send_payment:
    side_effect_class: non_idempotent_mutate
"""
    )
    calls: list[float] = []

    def send_payment(amount: float) -> dict[str, float]:
        calls.append(amount)
        return {"amount": amount}

    wrapped = cfg.apply_tool("send_payment", send_payment)
    runtime = SimpleNamespace(
        tool_call_id="call-1",
        execution_info=SimpleNamespace(thread_id="thread_1", run_id="run_lg"),
        config={"metadata": {"langgraph_node": "tools"}},
    )
    assert wrapped(10.0, runtime=runtime) == {"amount": 10.0}
    runtime_retry = SimpleNamespace(
        tool_call_id="call-2",
        execution_info=SimpleNamespace(thread_id="thread_1", run_id="run_lg"),
        config={"metadata": {"langgraph_node": "tools"}},
    )
    with pytest.raises(ToolBoundaryError):
        wrapped(10.0, runtime=runtime_retry)
    assert calls == [10.0]


def test_new_logical_run_gets_independent_guard_state() -> None:
    guard = _loop_guard(
        consecutive_soft={SideEffectClass.NON_IDEMPOTENT_MUTATE.value: 2}
    )
    calls = {"n": 0}

    @loop_guard_sync(
        guard, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE
    )
    def charge(*, tool_call_id: str) -> str:
        calls["n"] += 1
        return "ok"

    with execution_scope(_scope("checkpoint-run")):
        charge(tool_call_id="c0")
    with execution_scope(_scope("checkpoint-run")):
        with pytest.raises(ToolBoundaryError):
            charge(tool_call_id="c1")
    with execution_scope(_scope("new-logical-run")):
        charge(tool_call_id="c2")
    assert calls["n"] == 2


# ---------------------------------------------------------------------------
# F. Regression
# ---------------------------------------------------------------------------


def test_guards_disabled_missing_run_id_still_works() -> None:
    cfg = load_config_from_string(
        """
transition:
  agent_id: a
  policy_version: p
action_ledger:
  storage: memory
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
    )
    calls = {"n": 0}

    def charge() -> str:
        calls["n"] += 1
        return "ok"

    wrapped = cfg.apply_tool("charge", charge)
    assert wrapped(tool_call_id="c0") == "ok"
    assert wrapped(tool_call_id="c0") == "ok"
    assert calls["n"] == 1


def test_ledger_dedup_and_request_id_unchanged() -> None:
    from mycelium import ledger_sync
    from mycelium.transition import ToolTransitionBinding

    binding = ToolTransitionBinding.for_tool(
        agent_id="rid",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )
    calls = {"n": 0}

    @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
    def charge(amount: int) -> int:
        calls["n"] += 1
        return amount

    assert charge(10, request_id="charge-order:ORD-1") == 10
    assert charge(10, request_id="charge-order:ORD-1") == 10
    assert calls["n"] == 1


def test_missing_run_identity_error_is_exported() -> None:
    import mycelium

    assert mycelium.MissingRunIdentityError is MissingRunIdentityError
    err = MissingRunIdentityError(guard="LoopGuard", tool="search")
    assert err.guard == "LoopGuard"
    assert err.tool == "search"
    assert "Supply TransitionScope(run_id=...)" in str(err)
    assert "The protected tool was not executed" in str(err)
