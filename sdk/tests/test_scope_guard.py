"""Tests for ScopeGuard (AF-008) — run-level allowlist freeze."""

from __future__ import annotations

import pytest

from mycelium.action_ledger import LedgerHardBlockError
from mycelium.config import ConfigError, load_config_from_string
from mycelium.scope_guard import (
    ON_VIOLATION_HARD,
    VIOLATION_TOOL,
    InMemoryScopeGuardStorage,
    ScopeGrant,
    ScopeGuard,
    ScopeWidenRefusedError,
    scope_guard_sync,
)
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.tool_registry import ToolRegistry
from mycelium.transition import TransitionScope, execution_scope


def _scope(run_id: str = "run-1") -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id=run_id, node="tools")


def test_allowlisted_tool_passes() -> None:
    grant = ScopeGrant(allowed_tools=frozenset({"fetch_customer"}))
    guard = ScopeGuard(InMemoryScopeGuardStorage(), default_grant=grant)
    calls = {"n": 0}

    @scope_guard_sync(guard)
    def fetch_customer(customer_id: str) -> str:
        calls["n"] += 1
        return customer_id

    with execution_scope(_scope()):
        assert fetch_customer(customer_id="c1") == "c1"
    assert calls["n"] == 1


def test_tool_outside_frozen_allowlist_soft_blocks() -> None:
    grant = ScopeGrant(allowed_tools=frozenset({"fetch_customer"}))
    guard = ScopeGuard(InMemoryScopeGuardStorage(), default_grant=grant)
    calls = {"n": 0}

    @scope_guard_sync(guard, tool_name="delete_file")
    def delete_file(path: str) -> str:
        calls["n"] += 1
        return path

    with execution_scope(_scope()):
        with pytest.raises(ToolBoundaryError) as exc:
            delete_file(path="/workspace/src/a.py")
        assert exc.value.violation == VIOLATION_TOOL
    assert calls["n"] == 0


def test_mid_run_registry_widen_does_not_affect_frozen_grant() -> None:
    registry = ToolRegistry(allowed=["fetch_customer"])
    grant = ScopeGrant(allowed_tools=registry.allowed_tools)
    guard = ScopeGuard(InMemoryScopeGuardStorage(), default_grant=grant)

    @scope_guard_sync(guard, tool_name="admin_wipe")
    def admin_wipe() -> str:
        return "wiped"

    with execution_scope(_scope("run-widen")):
        guard.bind()
        registry.allow("admin_wipe")
        assert "admin_wipe" in registry.allowed_tools
        with pytest.raises(ToolBoundaryError) as exc:
            admin_wipe()
        assert exc.value.violation == VIOLATION_TOOL


def test_bind_refuses_widen_allows_narrow() -> None:
    narrow = ScopeGrant(allowed_tools=frozenset({"a"}))
    wide = ScopeGrant(allowed_tools=frozenset({"a", "b"}))
    guard = ScopeGuard(InMemoryScopeGuardStorage(), default_grant=narrow)

    with execution_scope(_scope("run-bind")):
        guard.bind()
        with pytest.raises(ScopeWidenRefusedError):
            guard.bind(grant=wide)
        state = guard.bind(grant=narrow)
        assert state.grant.allowed_tools == frozenset({"a"})


def test_hard_violation_mode() -> None:
    grant = ScopeGrant(allowed_tools=frozenset({"ok"}))
    guard = ScopeGuard(
        InMemoryScopeGuardStorage(),
        default_grant=grant,
        on_violation=ON_VIOLATION_HARD,
    )

    @scope_guard_sync(guard, tool_name="nope")
    def nope() -> None:
        return None

    with execution_scope(_scope()):
        with pytest.raises(LedgerHardBlockError):
            nope()


def test_missing_scope_skips_guard() -> None:
    grant = ScopeGrant(allowed_tools=frozenset({"ok"}))
    guard = ScopeGuard(InMemoryScopeGuardStorage(), default_grant=grant)
    calls = {"n": 0}

    @scope_guard_sync(guard, tool_name="nope")
    def nope() -> str:
        calls["n"] += 1
        return "x"

    nope()
    assert calls["n"] == 1


def test_yaml_apply_tool_freezes_registry_allowlist() -> None:
    cfg = load_config_from_string(
        """
registry:
  allowed: [fetch_customer]
scope_guard:
  storage: memory
tools:
  fetch_customer:
    bounded:
      schema:
        customer_id: { type: string, required: true }
  delete_file:
    bounded:
      schema:
        path: { type: string, required: true }
"""
    )
    guard = cfg.build_scope_guard()
    assert guard is not None
    assert guard.default_grant is not None
    assert guard.default_grant.allowed_tools == frozenset({"fetch_customer"})

    def fetch_customer(customer_id: str) -> str:
        return customer_id

    def delete_file(path: str) -> str:
        return path

    fetch = cfg.apply_tool("fetch_customer", fetch_customer)
    delete = cfg.apply_tool("delete_file", delete_file)

    with execution_scope(_scope("yaml-run")):
        assert fetch(customer_id="c9") == "c9"
        with pytest.raises(ToolBoundaryError) as exc:
            delete(path="/workspace/src/x.py")
        assert exc.value.violation == VIOLATION_TOOL


def test_yaml_from_tools_when_no_registry() -> None:
    cfg = load_config_from_string(
        """
scope_guard:
  storage: memory
  allowed_tools: all
tools:
  a:
    bounded:
      schema:
        x: { type: string, required: true }
  b:
    bounded:
      schema:
        y: { type: string, required: true }
"""
    )
    guard = cfg.build_scope_guard()
    assert guard is not None
    assert guard.default_grant is not None
    assert guard.default_grant.allowed_tools == frozenset({"a", "b"})


def test_yaml_requires_allowlist_source() -> None:
    with pytest.raises(ConfigError, match="non-empty allowlist"):
        load_config_from_string(
            """
scope_guard:
  storage: memory
tools: {}
"""
        )
