"""Tests for provider idempotency-key validity window.

When a tool declares ``provider_idempotency_key_ttl``, the ledger records
``provider_key_first_attempt_at`` on the first claim.  On a same-key retry of
a ``FAILED_BEFORE_EFFECT`` transition the gate checks whether the window has
expired — if so the provider may have purged its deduplication state and the
retry is hardened to ``HARD_BLOCK``.

Declaring both ``provider_idempotency_key_param`` and ``provider_idempotency_key_ttl``
also unlocks same-key retry on ``UNKNOWN`` while the window is ``VALID``.
Reconciler / operator release remain preferred before re-exec; expired keys
stay fail-closed.
"""

from __future__ import annotations

import time

import pytest

from mycelium import (
    ConfigError,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    ReconcileResult,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
    load_config_from_string,
    provider_key_validity,
    record_external_operation,
    side_effect,
)
from mycelium.transition import ProviderKeyValidity, RetryPermission
from mycelium.transition_resolution import (
    TransitionGate,
    hard_block_message,
    resolve_side_effect_gate,
)


def _keyed_binding(*, ttl: float | None = None) -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
        provider_idempotency_key_ttl=ttl,
    )


def _entry(
    provider_key: str | None,
    *,
    terminal_outcome: str = TerminalOutcome.FAILED_BEFORE_EFFECT.value,
    started_at: float | None = None,
    provider_key_first_attempt_at: float | None = None,
) -> LedgerEntry:
    return LedgerEntry(
        request_id="x",
        tool="send_payment",
        args=[],
        kwargs={},
        status="failed",
        terminal_outcome=terminal_outcome,
        side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
        provider_idempotency_key=provider_key,
        started_at=started_at if started_at is not None else time.time(),
        provider_key_first_attempt_at=provider_key_first_attempt_at,
    )


# --- provider_key_validity pure function -----------------------------------


def test_unconfigured_ttl_is_untracked() -> None:
    binding = _keyed_binding(ttl=None)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 999)
    assert provider_key_validity(entry, binding) == ProviderKeyValidity.UNTRACKED


def test_expired_key_is_expired() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 120.0)
    assert provider_key_validity(entry, binding) == ProviderKeyValidity.EXPIRED


def test_valid_key_within_window() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 30.0)
    assert provider_key_validity(entry, binding) == ProviderKeyValidity.VALID


def test_exact_boundary_not_expired() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 59.999)
    assert provider_key_validity(entry, binding) == ProviderKeyValidity.VALID


def test_falls_back_to_started_at_when_no_first_attempt_recorded() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry(
        "k1",
        provider_key_first_attempt_at=None,
        started_at=time.time() - 120.0,
    )
    assert provider_key_validity(entry, binding) == ProviderKeyValidity.EXPIRED


def test_no_ttl_on_binding_is_untracked() -> None:
    binding = _keyed_binding(ttl=None)
    entry = _entry("k1", provider_key_first_attempt_at=None)
    assert provider_key_validity(entry, binding) == ProviderKeyValidity.UNTRACKED


def test_now_override() -> None:
    binding = _keyed_binding(ttl=60.0)
    first = 1000.0
    entry = _entry("k1", provider_key_first_attempt_at=first)
    assert provider_key_validity(entry, binding, now=1050.0) == ProviderKeyValidity.VALID
    assert provider_key_validity(entry, binding, now=1061.0) == ProviderKeyValidity.EXPIRED


# --- gate-level: expired key hard-blocks -----------------------------------


def test_same_key_valid_ttl_allows() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 10.0)
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.ALLOW


def test_same_key_expired_ttl_hard_blocks() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 120.0)
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.HARD_BLOCK


def test_same_key_expired_ttl_idempotent_mutate_hard_blocks() -> None:
    binding = ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.IDEMPOTENT_MUTATE,
        retry_permission=RetryPermission.RETRY_ONLY_WITH_SAME_PROVIDER_IDEMPOTENCY_KEY,
        provider_idempotency_key_param="idempotency_key",
        provider_idempotency_key_ttl=60.0,
    )
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 120.0)
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.HARD_BLOCK


def test_different_key_hard_blocks_even_within_ttl() -> None:
    """Different key still hard-blocks (enforcement fires before TTL check)."""
    binding = _keyed_binding(ttl=60.0)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 10.0)
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k2",
        now=time.time(),
    )
    assert gate == TransitionGate.HARD_BLOCK


def test_untracked_ttl_ignored_by_gate() -> None:
    """No TTL → UNTRACKED → gate takes the existing ALLOW path unchanged."""
    binding = _keyed_binding(ttl=None)
    entry = _entry("k1", provider_key_first_attempt_at=time.time() - 999)
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.ALLOW


# --- UNKNOWN + same-key within validity window -----------------------------


def test_unknown_same_key_valid_ttl_allows() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry(
        "k1",
        terminal_outcome=TerminalOutcome.UNKNOWN.value,
        provider_key_first_attempt_at=time.time() - 10.0,
    )
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.ALLOW


def test_unknown_same_key_expired_ttl_hard_blocks() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry(
        "k1",
        terminal_outcome=TerminalOutcome.UNKNOWN.value,
        provider_key_first_attempt_at=time.time() - 120.0,
    )
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.HARD_BLOCK


def test_unknown_without_ttl_stays_hard_block() -> None:
    """TTL is the opt-in for UNKNOWN same-key retry — omit it → HARD_BLOCK."""
    binding = _keyed_binding(ttl=None)
    entry = _entry(
        "k1",
        terminal_outcome=TerminalOutcome.UNKNOWN.value,
        provider_key_first_attempt_at=time.time() - 10.0,
    )
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.HARD_BLOCK


def test_unknown_different_key_hard_blocks_within_ttl() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry(
        "k1",
        terminal_outcome=TerminalOutcome.UNKNOWN.value,
        provider_key_first_attempt_at=time.time() - 10.0,
    )
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k2",
        now=time.time(),
    )
    assert gate == TransitionGate.HARD_BLOCK


def test_blocked_never_allows_same_key_even_within_ttl() -> None:
    binding = _keyed_binding(ttl=60.0)
    entry = _entry(
        "k1",
        terminal_outcome=TerminalOutcome.BLOCKED.value,
        provider_key_first_attempt_at=time.time() - 10.0,
    )
    gate = resolve_side_effect_gate(
        entry,
        binding,
        incoming_provider_idempotency_key="k1",
        now=time.time(),
    )
    assert gate == TransitionGate.HARD_BLOCK


# --- hard_block_message includes key expiry info ---------------------------


def test_hard_block_message_includes_key_expiry() -> None:
    first = time.time() - 120.0
    binding = _keyed_binding(ttl=60.0)
    entry = _entry("k1", provider_key_first_attempt_at=first)
    msg = hard_block_message(
        entry,
        tool="send_payment",
        request_id="x",
        binding=binding,
        now=time.time(),
    )
    assert "provider_idempotency_key_ttl" in msg
    assert "age=" in msg
    assert "60.0" in msg


def test_hard_block_message_omits_key_expiry_without_binding() -> None:
    entry = _entry("k1")
    msg = hard_block_message(entry, tool="send_payment", request_id="x")
    assert "provider_idempotency_key_ttl" not in msg


# --- E2E through the ledger -------------------------------------------------


def test_expired_key_retry_hard_blocks() -> None:
    storage = InMemoryLedgerStorage()
    binding = _keyed_binding(ttl=0.0)  # expire immediately
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("gateway timeout before charge")
        return {"status": "sent"}

    scope = TransitionScope(thread_id="t1", run_id="r1")
    with execution_scope(scope):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        with pytest.raises(LedgerHardBlockError, match="manual reconciliation"):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert attempts["n"] == 1  # second body never executed


def test_valid_key_retry_succeeds() -> None:
    storage = InMemoryLedgerStorage()
    binding = _keyed_binding(ttl=300.0)
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("gateway timeout before charge")
        return {"status": "sent"}

    scope = TransitionScope(thread_id="t1", run_id="r1")
    with execution_scope(scope):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        result = send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert attempts["n"] == 2
    assert result == {"status": "sent"}


class _StubReconciler:
    def __init__(self, result: ReconcileResult) -> None:
        self._result = result
        self.calls: list[str] = []

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        self.calls.append(entry.request_id)
        return self._result


def test_unknown_same_key_valid_ttl_retries_without_reconciler() -> None:
    storage = InMemoryLedgerStorage()
    binding = _keyed_binding(ttl=300.0)
    attempts = {"n": 0}
    claims: list[tuple[int, float | None]] = []

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        claimed = storage.list_all()[0]
        claims.append((claimed.fence, claimed.lease_until))
        with side_effect():
            if attempts["n"] == 1:
                raise RuntimeError("timeout after maybe-crossed")
            return {"status": "sent"}

    scope = TransitionScope(thread_id="t1", run_id="r1")
    with execution_scope(scope):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        result = send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert attempts["n"] == 2
    assert result == {"status": "sent"}
    assert [fence for fence, _lease_until in claims] == [1, 2]
    assert claims[1][1] is not None


def test_unknown_same_key_expired_ttl_hard_blocks_e2e() -> None:
    storage = InMemoryLedgerStorage()
    binding = _keyed_binding(ttl=0.0)
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        with side_effect():
            raise RuntimeError("timeout after maybe-crossed")

    scope = TransitionScope(thread_id="t1", run_id="r1")
    with execution_scope(scope):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        with pytest.raises(LedgerHardBlockError, match="manual reconciliation"):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert attempts["n"] == 1


def test_unknown_same_key_prefers_reconciler_over_reexec() -> None:
    storage = InMemoryLedgerStorage()
    binding = _keyed_binding(ttl=300.0)
    reconciler = _StubReconciler(ReconcileResult.completed({"status": "settled"}))
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding, reconciler=reconciler)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        with side_effect():
            record_external_operation("pi_unknown_1")
            raise RuntimeError("timeout after maybe-crossed")

    scope = TransitionScope(thread_id="t1", run_id="r1")
    with execution_scope(scope):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        result = send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert result == {"status": "settled"}
    assert attempts["n"] == 1
    assert len(reconciler.calls) == 1


def test_unknown_without_ttl_still_hard_blocks_e2e() -> None:
    storage = InMemoryLedgerStorage()
    binding = _keyed_binding(ttl=None)
    attempts = {"n": 0}

    @ledger_sync(storage=storage, transition_binding=binding)
    def send_payment(amount: float, idempotency_key: str) -> dict[str, str]:
        attempts["n"] += 1
        with side_effect():
            raise RuntimeError("timeout after maybe-crossed")

    scope = TransitionScope(thread_id="t1", run_id="r1")
    with execution_scope(scope):
        with pytest.raises(RuntimeError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

        with pytest.raises(LedgerHardBlockError):
            send_payment(amount=10.0, idempotency_key="k1", tool_call_id="c1")

    assert attempts["n"] == 1


# --- serialisation round-trip ----------------------------------------------


def test_entry_round_trips_provider_key_first_attempt_at() -> None:
    ts = time.time() - 100.0
    entry = LedgerEntry(
        request_id="r1",
        tool="send_payment",
        args=[],
        kwargs={},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        provider_idempotency_key="k1",
        provider_key_first_attempt_at=ts,
    )
    restored = LedgerEntry.from_dict(entry.to_dict())
    assert restored.provider_key_first_attempt_at == ts


def test_legacy_entry_missing_field_is_none() -> None:
    legacy = {
        "request_id": "r1",
        "tool": "send_payment",
        "args": [],
        "kwargs": {},
        "status": "completed",
        "terminal_outcome": TerminalOutcome.COMPLETED.value,
        "provider_idempotency_key": "k1",
    }
    restored = LedgerEntry.from_dict(legacy)
    assert restored.provider_key_first_attempt_at is None


# --- config wiring ----------------------------------------------------------


def test_config_parses_and_binds_ttl() -> None:
    yaml_text = """
transition:
  agent_id: demo
  policy_version: "1"
action_ledger:
  storage: memory
  tools: [send_payment]
tools:
  send_payment:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
    provider_idempotency_key_ttl: 3600
"""
    config = load_config_from_string(yaml_text)
    tool_config = config.tools["send_payment"]
    assert tool_config.provider_idempotency_key_ttl == 3600.0

    binding = config.tool_transition_binding(tool_config)
    assert binding is not None
    assert binding.provider_idempotency_key_ttl == 3600.0


def test_config_ttl_omitted_defaults_none() -> None:
    yaml_text = """
transition:
  agent_id: demo
  policy_version: "1"
tools:
  send_payment:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
"""
    config = load_config_from_string(yaml_text)
    tool_config = config.tools["send_payment"]
    assert tool_config.provider_idempotency_key_ttl is None


def test_config_rejects_non_positive_ttl() -> None:
    for yaml_text, desc in [
        (
            """
transition:
  agent_id: demo
  policy_version: "1"
tools:
  send_payment:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
    provider_idempotency_key_ttl: 0
""",
            "zero",
        ),
        (
            """
transition:
  agent_id: demo
  policy_version: "1"
tools:
  send_payment:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
    provider_idempotency_key_ttl: -1
""",
            "negative",
        ),
        (
            """
transition:
  agent_id: demo
  policy_version: "1"
tools:
  send_payment:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
    provider_idempotency_key_ttl: abc
""",
            "non-numeric",
        ),
    ]:
        with pytest.raises(ConfigError, match="provider_idempotency_key_ttl"):
            load_config_from_string(yaml_text)
