"""Tests for StateAuthority (superseded-state execution gate)."""

from __future__ import annotations

from pathlib import Path

import pytest

from mycelium.action_ledger import (
    ActionLedger,
    InMemoryLedgerStorage,
    LedgerHardBlockError,
    ledger_sync,
)
from mycelium.config import ConfigError, load_config_from_string
from mycelium.state_authority import (
    VIOLATION_MISSING,
    VIOLATION_SUPERSEDED,
    StateAuthority,
    extract_decision_id,
    extract_state_ref,
    state_authority_sync,
)
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import (
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    args_fingerprint,
    derive_transition_key_for_call,
    execution_scope,
)


def _scope(run_id: str = "run-1") -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id=run_id, node="tools")


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        policy_version="2026.08.1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
        agent_id="test",
    )


def test_extract_helpers() -> None:
    assert extract_state_ref({"state_ref": "ckpt-1"}) == "ckpt-1"
    assert extract_state_ref({"state_ref": ""}) is None
    assert extract_decision_id({"decision_id": "dec-9"}) == "dec-9"
    assert extract_decision_id({}) is None


def test_matching_state_ref_allows() -> None:
    calls = {"n": 0}
    authority = StateAuthority(
        lambda **_: "ckpt-1",
        require_state_ref=True,
    )

    @state_authority_sync(authority)
    def refund(amount: float, *, tool_call_id: str, state_ref: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope()):
        assert refund(10.0, tool_call_id="c1", state_ref="ckpt-1") == 10.0
    assert calls["n"] == 1


def test_superseded_state_hard_blocks_before_body() -> None:
    calls = {"n": 0}
    authority = StateAuthority(
        lambda **_: "ckpt-2",
        require_state_ref=True,
        on_mismatch="hard",
    )

    @state_authority_sync(authority)
    def refund(amount: float, *, tool_call_id: str, state_ref: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope()):
        with pytest.raises(LedgerHardBlockError, match="superseded"):
            refund(10.0, tool_call_id="c_new", state_ref="ckpt-1")
    assert calls["n"] == 0


def test_superseded_state_soft_blocks() -> None:
    authority = StateAuthority(
        lambda **_: "ckpt-2",
        on_mismatch="soft",
    )

    @state_authority_sync(authority)
    def refund(amount: float, *, tool_call_id: str, state_ref: str) -> float:
        return amount

    with execution_scope(_scope()):
        with pytest.raises(ToolBoundaryError) as exc:
            refund(10.0, tool_call_id="c_new", state_ref="ckpt-1")
    assert exc.value.violation == VIOLATION_SUPERSEDED
    assert "superseded" in exc.value.llm_message.lower()


def test_missing_state_ref_required() -> None:
    authority = StateAuthority(
        lambda **_: "ckpt-1",
        require_state_ref=True,
        on_missing="soft",
    )

    @state_authority_sync(authority)
    def refund(amount: float, *, tool_call_id: str) -> float:
        return amount

    with execution_scope(_scope()):
        with pytest.raises(ToolBoundaryError) as exc:
            refund(10.0, tool_call_id="c1")
    assert exc.value.violation == VIOLATION_MISSING


def test_missing_state_ref_optional_allows() -> None:
    calls = {"n": 0}
    authority = StateAuthority(lambda **_: "ckpt-1", require_state_ref=False)

    @state_authority_sync(authority)
    def refund(amount: float, *, tool_call_id: str) -> float:
        calls["n"] += 1
        return amount

    with execution_scope(_scope()):
        assert refund(5.0, tool_call_id="c1") == 5.0
    assert calls["n"] == 1


def test_proof_stale_checkpoint_new_tool_call_id_ledger_allows_gate_blocks() -> None:
    """The named gap: stale S0 + new tool_call_id → ledger PROCEED; gate blocks.

    Without StateAuthority, a redispatch from superseded checkpoint S0 that
    mints a new tool_call_id (and optionally different args) has no prior
    claim, so the ActionLedger allows execution. With the gate, the frozen
    state_ref fails the canonical compare and the body never runs / never
    claims.
    """
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage)
    binding = _binding()
    body_calls = {"n": 0}
    canonical = {"ref": "ckpt-S0"}

    def current_ref(**_: object) -> str:
        return str(canonical["ref"])

    authority = StateAuthority(current_ref, require_state_ref=True, on_mismatch="hard")

    @state_authority_sync(authority, side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE)
    @ledger_sync(storage=storage, transition_binding=binding)
    def refund(amount: float) -> float:
        body_calls["n"] += 1
        return amount

    with execution_scope(_scope()):
        # Decision at S0 executes once.
        assert (
            refund(
                amount=50.0,
                tool_call_id="call_s0",
                state_ref="ckpt-S0",
                decision_id="dec-s0",
            )
            == 50.0
        )
        assert body_calls["n"] == 1
        entry = ledger.get(
            derive_transition_key_for_call(
                "refund",
                (),
                {
                    "amount": 50.0,
                    "tool_call_id": "call_s0",
                    "state_ref": "ckpt-S0",
                    "decision_id": "dec-s0",
                },
                binding,
            )
        )
        assert entry is not None
        assert entry.state_ref == "ckpt-S0"
        assert entry.decision_id == "dec-s0"

        # Canonical state advances (partner / another worker / later checkpoint).
        canonical["ref"] = "ckpt-S1"

        # Stale redispatch: new tool_call_id, maybe changed args — no prior claim.
        # Ledger alone would PROCEED; StateAuthority hard-blocks first.
        with pytest.raises(LedgerHardBlockError, match="superseded"):
            refund(
                amount=75.0,
                tool_call_id="call_stale_new",
                state_ref="ckpt-S0",
                decision_id="dec-s0",
            )
        assert body_calls["n"] == 1

        # Prove ledger would have treated this as a *new* transition (gap without gate).
        new_key = derive_transition_key_for_call(
            "refund",
            (),
            {
                "amount": 75.0,
                "tool_call_id": "call_stale_new",
                "state_ref": "ckpt-S0",
                "decision_id": "dec-s0",
            },
            binding,
        )
        assert ledger.get(new_key) is None

        # Control: ledger-only path with the same new id still has no prior claim.
        @ledger_sync(storage=InMemoryLedgerStorage(), transition_binding=binding)
        def refund_ledger_only(amount: float) -> float:
            body_calls["ledger_only"] = body_calls.get("ledger_only", 0) + 1
            return amount

        assert (
            refund_ledger_only(
                amount=75.0, tool_call_id="call_stale_new", state_ref="ckpt-S0"
            )
            == 75.0
        )
        assert body_calls["ledger_only"] == 1


def test_state_ref_excluded_from_args_fingerprint() -> None:
    a = args_fingerprint((), {"amount": 10.0, "state_ref": "a", "tool_call_id": "c1"})
    b = args_fingerprint((), {"amount": 10.0, "state_ref": "b", "tool_call_id": "c1"})
    c = args_fingerprint((), {"amount": 10.0, "decision_id": "d1", "tool_call_id": "c1"})
    assert a == b == c


def test_claim_stores_decision_passthrough() -> None:
    ledger = ActionLedger(InMemoryLedgerStorage())
    binding = _binding()
    with execution_scope(_scope()):
        rid = derive_transition_key_for_call(
            "refund",
            (),
            {
                "amount": 1.0,
                "tool_call_id": "c1",
                "state_ref": "s1",
                "decision_id": "d1",
            },
            binding,
        )
        entry = ledger.claim_side_effecting(
            rid,
            "refund",
            (),
            {
                "amount": 1.0,
                "tool_call_id": "c1",
                "state_ref": "s1",
                "decision_id": "d1",
            },
            binding,
        )
    assert entry.state_ref == "s1"
    assert entry.decision_id == "d1"
    round_trip = type(entry).from_dict(entry.to_dict())
    assert round_trip.state_ref == "s1"
    assert round_trip.decision_id == "d1"


def test_yaml_state_authority_wires_gate(tmp_path: Path) -> None:
    resolver_mod = tmp_path / "canon_mod.py"
    resolver_mod.write_text(
        "CANONICAL = {'ref': 'ckpt-live'}\n"
        "def get_canonical_state_ref(*, tool, thread_id, run_id, kwargs):\n"
        "    return CANONICAL['ref']\n",
        encoding="utf-8",
    )
    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        cfg = load_config_from_string(
            """
transition:
  agent_id: test
  policy_version: "2026.08.1"
state_authority:
  canonical_callable: canon_mod:get_canonical_state_ref
  require_state_ref: true
  on_mismatch: hard
tools:
  refund:
    side_effect_class: non_idempotent_mutate
"""
        )

        calls = {"n": 0}

        def refund(amount: float, *, tool_call_id: str, state_ref: str) -> float:
            calls["n"] += 1
            return amount

        wrapped = cfg.apply_tool("refund", refund)
        with execution_scope(_scope()):
            with pytest.raises(LedgerHardBlockError):
                wrapped(10.0, tool_call_id="c1", state_ref="ckpt-stale")
        assert calls["n"] == 0
        with execution_scope(_scope()):
            assert wrapped(10.0, tool_call_id="c2", state_ref="ckpt-live") == 10.0
        assert calls["n"] == 1
    finally:
        sys.path.remove(str(tmp_path))


def test_yaml_requires_canonical_callable() -> None:
    with pytest.raises(ConfigError, match="canonical_callable"):
        load_config_from_string(
            """
state_authority:
  require_state_ref: true
tools:
  refund:
    side_effect_class: non_idempotent_mutate
"""
        )
